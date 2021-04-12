namespace Propulsion.EventStoreDB

open System
open System.Text
open EventStore.ClientAPI
open EventStore.ClientAPI.SystemData
open Microsoft.FSharp.Control

open Propulsion.Streams
open Propulsion.EventStoreDB.Stats
open Propulsion.EventStoreDB.StreamReader

/// `AllStreamReader` will read from the `$all` stream and track the
/// `OriginalPosition` of any processed events.
type AllStreamReader
    (
        logger: Serilog.ILogger,
        conn: IEventStoreConnection,
        checkpointer: ICheckpointer,
        submitBatch: SubmitBatchHandler,
        eventFilter: ResolvedEvent -> bool,
        credentials: UserCredentials,
        consumerGroup,
        maxBatchSize: int,
        tailSleepInterval: TimeSpan,
        stats: IStats
    ) =

    let streamName = AllStreamName

    member this.Commit position = async {
        try
            do! checkpointer.CommitPosition streamName consumerGroup position
            stats.UpdateCommitedPosition position
            logger.Debug ("Committed position {position}", position)
        with
        | ex ->
            logger.Error (ex, "Exception while commiting position {position}", position)
            return! Async.Raise ex
    }

    member this.ProcessEvents (events: ResolvedEvent []) isEndOfStream = async {
        match events |> Array.filter eventFilter with
        | events when Array.isEmpty events ->
            logger.Debug ("Empty batch retrieved")
            stats.UpdateEmptySlice ()

        | events ->
            let streamEvents =
                events
                |> Seq.map streamEventFromResolvedEvent
                |> Array.ofSeq

            let batch =
                {
                    events        = streamEvents
                    firstPosition = events |> Seq.head |> getEventPosition
                    lastPosition  = events |> Seq.last |> getEventPosition
                    isEnd         = isEndOfStream
                }

            logger.Information ("Submitting batch of {batchSize} events (of {totalEvents}), position {firstPosition} through {lastPosition}",
                batch.Length, events.Length, batch.firstPosition, batch.lastPosition)

            do stats.UpdateBatch batch

            let! cur, max =
                submitBatch (batch.lastPosition, this.Commit batch.lastPosition, batch.events)

            do stats.UpdateCurMax cur max
    }

    interface IStreamReader with
        member this.Start (committedPosition: Nullable<int64>) = async {
            // Start reporting stats
            do! Async.StartChild (stats.Start ()) |> Async.Ignore

            let mutable work =
                if committedPosition.HasValue then
                    let position = Position (committedPosition.Value, committedPosition.Value)
                    logger.Information (
                        "Continuing reading stream {streamName} from position {position}, maxBatchSize {maxBatchSize}",
                         streamName, position.CommitPosition, maxBatchSize)
                    AllStreamWork.TakeInitial position
                else
                    let position = Position.Start
                    logger.Information (
                        "Starting reading stream {streamName} from the beginning, maxBatchSize {maxBatchSize}",
                         streamName, maxBatchSize)
                    AllStreamWork.TakeInitial position

            let! ct = Async.CancellationToken
            while not ct.IsCancellationRequested do
                let! eventsSlice =
                    match work with
                    | AllStreamWork.TakeInitial position ->
                        async {
                            let! eventsSlice =
                                conn.ReadAllEventsForwardAsync (position, maxBatchSize, true, credentials)
                                |> Async.AwaitTaskCorrect

                            /// Filter the event with the same position as what we're continuing from
                            let events =
                                eventsSlice.Events
                                |> Array.filter (fun event -> not (event.OriginalPosition.Value.CommitPosition = position.CommitPosition))

                            do! this.ProcessEvents events eventsSlice.IsEndOfStream

                            return eventsSlice
                        }

                    /// TakeNext is invoked by `eventsSlice.NextPosition` and therefore doesn't
                    /// include duplicate events
                    | AllStreamWork.TakeNext position ->
                        async {
                            let! eventsSlice =
                                conn.ReadAllEventsForwardAsync (position, maxBatchSize, true, credentials)
                                |> Async.AwaitTaskCorrect

                            let events = eventsSlice.Events

                            do! this.ProcessEvents events eventsSlice.IsEndOfStream

                            return eventsSlice
                        }

                work <- AllStreamWork.TakeNext eventsSlice.NextPosition
                if eventsSlice.IsEndOfStream then do! Async.Sleep tailSleepInterval
        }
