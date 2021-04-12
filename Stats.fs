namespace Propulsion.EventStoreDB

open System
open Propulsion.Streams
open Serilog

module Stats =

    [<NoComparison; NoEquality>]
    type InternalBatch =
        {
            firstPosition  : int64
            lastPosition   : int64
            events         : StreamEvent<byte []> []
            isEnd          : bool
        }
        member this.Length = this.events.Length

    type IStats =
        abstract Start: unit -> Async<unit>
        abstract UpdateBatch: InternalBatch -> unit
        abstract UpdateEmptySlice: unit -> unit
        abstract UpdateCommitedPosition: int64 -> unit
        abstract UpdateCurMax: int -> int -> unit

    type Stats (logger: ILogger, statsInterval: TimeSpan) =
        let mutable batchFirstPosition = 0L
        let mutable batchLastPosition = 0L
        let mutable batchCaughtUp = false

        let mutable slicesRead = 0
        let mutable slicesEmpty = 0
        let mutable recentSlicesRead = 0
        let mutable recentSlicesEmpty = 0

        let mutable currentBatches = 0
        let mutable maxBatches = 0

        let mutable lastCommittedPosition = 0L

        let report () =
            logger.Information (
                "Slices Read {slicesRead} Empty {slicesEmpty} | Recent Read {recentSlicesRead} Empty {recentSlicesEmpty} | Position Read {batchLastPosition} Committed {lastCommittedPosition} | Caught up {caughtUp} | cur {cur} / max {max}",
                slicesRead, slicesEmpty, recentSlicesRead, recentSlicesEmpty, batchLastPosition, lastCommittedPosition, batchCaughtUp, currentBatches, maxBatches)

        interface IStats with
            member __.UpdateBatch (batch: InternalBatch) =
                batchFirstPosition <- batch.firstPosition
                batchLastPosition <- batch.lastPosition
                batchCaughtUp <- batch.isEnd

                slicesRead <- slicesRead + 1
                recentSlicesRead <- recentSlicesRead + 1

            member __.UpdateEmptySlice () =
                slicesEmpty <- slicesEmpty + 1
                recentSlicesEmpty <- recentSlicesEmpty + 1

            member __.UpdateCommitedPosition pos =
                lastCommittedPosition <- pos

            member __.UpdateCurMax cur max =
                currentBatches <- cur
                maxBatches <- max

            member __.Start () = async {
                let! ct = Async.CancellationToken
                while not ct.IsCancellationRequested do
                    report ()
                    recentSlicesRead <- 0
                    recentSlicesEmpty <- 0
                    do! Async.Sleep statsInterval
                }