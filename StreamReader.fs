namespace Propulsion.EventStoreDB

open Propulsion.Streams
open EventStore.ClientAPI

open System

module StreamReader =
    open EventStore.ClientAPI.SystemData

    let [<Literal>] AllStreamName = "$all"

    [<RequireQualifiedAccess; NoComparison; NoEquality>]
    type StreamType =
        /// Stream the "$all" stream. This requires explicit user credentials
        | AllStream of userCredentials: UserCredentials
        /// Stream a named stream
        | NamedStream of name: string

    [<RequireQualifiedAccess; NoComparison; NoEquality>]
    type AllStreamWork =
        | TakeInitial of Position
        | TakeNext of Position

    [<RequireQualifiedAccess; NoComparison; NoEquality>]
    type NamedStreamWork =
        | TakeInitial of int64
        | TakeNext of int64

    type SubmitBatchHandler =
        // ingester submit method: epoch * checkpoint * items -> write result
        int64 * Async<unit> * seq<Propulsion.Streams.StreamEvent<byte[]>> -> Async<int*int>

    type IStreamReader =
        abstract member Start : Nullable<int64> -> Async<unit>

    /// Get the a `ResolvedEvent`'s `OriginalEventNumber` property.
    /// This is used to checkpoint the position of an event from a
    /// `StreamEventsSlice`.
    let getEventNumber (event: ResolvedEvent) = event.Event.EventNumber

    /// Get a `ResolvedEvent`'s `OriginalPosition` property.
    /// This is used to checkpoint the position of an event from an
    /// `AllEventsSlice`.
    let getEventPosition (event: ResolvedEvent) = event.OriginalPosition.Value.CommitPosition

    let streamEventFromResolvedEvent (resolvedEvent: ResolvedEvent) : StreamEvent<_> =
        let event = resolvedEvent.Event

        let timelineEvent =
            FsCodec.Core.TimelineEvent.Create (
                index     = getEventNumber resolvedEvent,
                eventType = event.EventType,
                data      = Array.emptyAsNull event.Data,
                meta      = Array.emptyAsNull event.Metadata,
                eventId   = event.EventId,
                timestamp = DateTimeOffset.FromUnixTimeMilliseconds event.CreatedEpoch
            )

        {
            stream = StreamName.internalParseSafe event.EventStreamId
            event = timelineEvent
        }

