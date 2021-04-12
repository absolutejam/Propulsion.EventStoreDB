namespace Propulsion.EventStoreDB

open System
open EventStore.ClientAPI

open Propulsion.EventStoreDB.Stats
open Propulsion.EventStoreDB.StreamReader

type StartPos =
    | Start
    | Continue
    | Absolute of int64

type EventStoreSource =

    /// Create and run an `IStreamReader`
    static member Run
        (
            readerLogger: Serilog.ILogger,
            statsLogger: Serilog.ILogger,
            conn: IEventStoreConnection,
            startPosition: StartPos,
            checkpointer: ICheckpointer,
            consumerGroup: string,
            maxBatchSize: int,
            sink: Propulsion.ProjectorPipeline<_>,
            eventFilter: ResolvedEvent -> bool,
            tailSleepInterval: TimeSpan,
            statsInterval: TimeSpan,
            streamType: StreamType,
            ?stats: IStats
        )
        : Async<unit>
        = async {

            let statsLogger =
                let instanceId = Guid.NewGuid()
                statsLogger
                    .ForContext("instanceId", string instanceId)
                    .ForContext("consumerGroup", consumerGroup)

            let ingester : Propulsion.Ingestion.Ingester<_,_> =
                sink.StartIngester (statsLogger, 0)

            let stats = stats |> Option.defaultWith (fun () -> Stats (statsLogger, statsInterval) :> IStats)

            let streamName, reader =
                match streamType with
                | StreamType.AllStream credentials ->
                    let reader =
                        AllStreamReader (
                            readerLogger,
                            conn,
                            checkpointer,
                            ingester.Submit,
                            eventFilter,
                            credentials,
                            consumerGroup,
                            maxBatchSize,
                            tailSleepInterval,
                            stats
                        ) :> IStreamReader

                    AllStreamName, reader

                | StreamType.NamedStream streamName  ->
                    let reader =
                        NamedStreamReader (
                            readerLogger,
                            conn,
                            checkpointer,
                            ingester.Submit,
                            eventFilter,
                            streamName,
                            consumerGroup,
                            maxBatchSize,
                            tailSleepInterval,
                            stats
                        ) :> IStreamReader

                    streamName, reader

            try
                let! position = async {
                    match startPosition with
                    | Absolute startPos -> return Nullable startPos
                    | Start             -> return Nullable ()
                    | Continue          -> return! checkpointer.GetPosition streamName consumerGroup
                }

                do! reader.Start position
            with
            | ex ->
                readerLogger.Fatal (ex, "Exception encountered while running reader, exiting loop")
                return! Async.Raise ex
        }
