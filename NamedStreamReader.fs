namespace Propulsion.EventStoreDB

open System
open EventStore.ClientAPI
open Microsoft.FSharp.Control

open Propulsion.EventStoreDB.Stats
open Propulsion.EventStoreDB.StreamReader

/// TODO: This is currently untested
type NamedStreamReader
    (
        logger: Serilog.ILogger,
        conn: IEventStoreConnection,
        checkpointer: ICheckpointer,
        submitBatch: SubmitBatchHandler,
        eventFilter: ResolvedEvent -> bool,
        streamName,
        consumerGroup,
        maxBatchSize: int,
        tailSleepInterval: TimeSpan,
        stats: IStats
    ) =

    member this.Commit position = async {
        try
            do! checkpointer.CommitPosition streamName consumerGroup position
            stats.UpdateCommitedPosition position
            logger.Warning ("Committed position {position}", position)
        with
        | ex ->
            logger.Warning (ex, "Exception while commiting position {position}", position)
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
                    firstPosition = events |> Array.head |> getEventNumber
                    lastPosition  = events |> Array.last |> getEventNumber
                    isEnd         = isEndOfStream
                }

            logger.Debug ("Submitting a batch of {batchSize} events, position {firstPosition} through {lastPosition}",
                batch.Length, batch.firstPosition, batch.lastPosition)

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
                    let position = committedPosition.Value
                    logger.Information (
                        "Continuing reading stream {streamName} from position {position}, maxBatchSize {maxBatchSize}",
                         streamName, position, maxBatchSize)
                    NamedStreamWork.TakeInitial position
                else
                    let position = int64 StreamPosition.Start
                    logger.Information (
                        "Starting reading stream {streamName} from the beginning, maxBatchSize {maxBatchSize}",
                         streamName, maxBatchSize)
                    NamedStreamWork.TakeInitial position

            let! ct = Async.CancellationToken
            while not ct.IsCancellationRequested do
                let! eventsSlice =
                    match work with
                    | NamedStreamWork.TakeInitial position ->
                        async {
                            let! eventsSlice =
                                conn.ReadStreamEventsForwardAsync (streamName, position, maxBatchSize, true)
                                |> Async.AwaitTaskCorrect

                            /// Filter the event with the same position as what we're continuing from
                            let events =
                                eventsSlice.Events
                                |> Array.filter (fun event -> not (event.OriginalPosition.Value.CommitPosition = position))

                            do! this.ProcessEvents events eventsSlice.IsEndOfStream

                            return eventsSlice
                        }

                    | NamedStreamWork.TakeNext position ->
                        async {
                            let! eventsSlice =
                                conn.ReadStreamEventsForwardAsync (streamName, position, maxBatchSize, true)
                                |> Async.AwaitTaskCorrect

                            let events = eventsSlice.Events

                            do! this.ProcessEvents events eventsSlice.IsEndOfStream

                            return eventsSlice
                        }

                work <- NamedStreamWork.TakeNext eventsSlice.NextEventNumber
                if eventsSlice.IsEndOfStream then do! Async.Sleep tailSleepInterval
        }
