namespace Propulsion.EventStoreDB

open System
open System.Text
open Newtonsoft.Json
open EventStore.ClientAPI
open System.Collections.Concurrent

[<CLIMutable; NoComparison>]
type CheckpointEntry =
    {
        Stream:        string
        ConsumerGroup: string
        Position:      Nullable<int64>
    }

type ICheckpointer =
    /// Get the position for a stream
    abstract member GetPosition: stream: string -> consumerGroup: string -> Async<Nullable<int64>>
    /// Commit the position for a stream
    abstract member CommitPosition: stream: string -> consumerGroup: string -> position: int64 -> Async<unit>

[<RequireQualifiedAccess>]
module EventStoreCheckpointer =
    let [<Literal>] CheckpointSuffix = "checkpoint"

    /// Builds a stream name for the checkpoint stream based upon the stream and consumer group.
    /// Also replaces any leading `$` with `_` as not to conflict with EventStore built-in streams.
    let buildCheckpointStreamName (stream: string) (consumerGroup: string) =
        sprintf "%s-%s_%s" (stream.TrimStart ('$', '_')) consumerGroup CheckpointSuffix

    /// Serialize & encode an object to bytes
    let encode (object: obj) = JsonConvert.SerializeObject object |> Encoding.UTF8.GetBytes

    /// Decode & deserialize a byte array to a `CheckpointEntry`
    let decode<'t> (data: byte array) = Encoding.UTF8.GetString data |> JsonConvert.DeserializeObject<'t>


/// `ICheckpointer` implementation that holds checkpoints in an in-memory ConcurrentDictionary
type InMemoryCheckpointer
    (
        ?checkpointStreamNamer: string -> string -> string,
        ?logger: Serilog.ILogger
    ) =
    let store = ConcurrentDictionary<string, int64>()
    let buildCheckpointStreamName =
        checkpointStreamNamer
        |> Option.defaultValue EventStoreCheckpointer.buildCheckpointStreamName

    interface ICheckpointer with
        member __.GetPosition (stream: string) (consumerGroup: string) = async {
            let checkpointStream = buildCheckpointStreamName stream consumerGroup
            match store.TryGetValue checkpointStream with
            | true, position ->
                logger |> Option.iter (fun logger -> logger.Debug ("Getting position for {checkpointStream}: {position}", checkpointStream, position))
                return Nullable position
            | false, _ ->
                logger |> Option.iter (fun logger -> logger.Debug ("Getting position for {checkpointStream}: Null", checkpointStream))
                return Nullable ()
        }

        member __.CommitPosition (stream: string) (consumerGroup: string) (position: int64) = async {
            let checkpointStream = buildCheckpointStreamName stream consumerGroup
            logger |> Option.iter (fun logger -> logger.Debug ("Committing position for {checkpointStream}: {position}", checkpointStream, position))
            store.AddOrUpdate (checkpointStream, position, fun _ _ -> position) |> ignore<int64>
            return ()
        }


/// `ICheckpointer` implementation that stores checkpoints in EventStore
type EventStoreCheckpointer
    (
        conn: IEventStoreConnection,
        logger: Serilog.ILogger,
        ?checkpointStreamNamer: string -> string -> string
    ) =

    /// Use the provided `checkpointStreamNamer` function or use the default if not provided
    let buildCheckpointStreamName =
        checkpointStreamNamer
        |> Option.defaultValue EventStoreCheckpointer.buildCheckpointStreamName

    /// Create the associated checkpoint stream for the provided stream
    member this.CreateCheckpointStream (stream: string) (consumerGroup: string) = async {
        let expectedVersion = int64 ExpectedVersion.Any
        let checkpointStream = buildCheckpointStreamName stream consumerGroup

        let metadata =
            StreamMetadata
                .Build()
                .SetMaxCount(1L)
                .SetCustomProperty("consumerGroup", consumerGroup)
                .SetCustomProperty("sourceStream", stream)
                .Build()

        return!
            conn.SetStreamMetadataAsync (checkpointStream, expectedVersion, metadata)
            |> Async.AwaitTaskCorrect
            |> Async.Ignore // TODO: ??
    }

    member this.CreateCheckpointEvent stream consumerGroup position =
        let metadata = {| |}
        let data =
            {
                Stream        = stream
                ConsumerGroup = consumerGroup
                Position      = Nullable position
            }

        EventData (
            eventId=Guid.NewGuid (),
            ``type``="checkpoint",
            isJson=true,
            data=EventStoreCheckpointer.encode data,
            metadata=EventStoreCheckpointer.encode metadata
        )


    interface ICheckpointer with
        member this.GetPosition (stream: string) (consumerGroup: string) = async {
            let position = int64 StreamPosition.End
            let checkpointStream = buildCheckpointStreamName stream consumerGroup

            let! event =
                conn.ReadEventAsync (checkpointStream, position, false)
                |> Async.AwaitTaskCorrect

            match event.Status with
            | EventReadStatus.NotFound
            | EventReadStatus.NoStream ->
                logger.Information ("Creating stream {checkpointStream}", checkpointStream)
                do! this.CreateCheckpointStream stream consumerGroup
                return Nullable ()

            | _ ->
                let eventData = EventStoreCheckpointer.decode<CheckpointEntry> event.Event.Value.Event.Data
                let position = eventData.Position
                logger.Debug ("Got position {position} for {checkpointStream}", checkpointStream, position)
                return position
        }

        member this.CommitPosition (stream: string) (consumerGroup: string) (position: int64) = async {
            let expectedVersion = int64 ExpectedVersion.Any
            let checkpointStream = buildCheckpointStreamName stream consumerGroup
            let checkpointEvent = this.CreateCheckpointEvent stream consumerGroup position

            logger.Debug ("Committing position for {checkpointStream}: {position}", checkpointStream, position)

            return!
                conn.AppendToStreamAsync (checkpointStream, expectedVersion, checkpointEvent)
                |> Async.AwaitTaskCorrect
                |> Async.Ignore
        }
