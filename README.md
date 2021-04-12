# Propulsion.EventStoreDB

This library provides the ability to read a named or `$all` stream from
[EventStore](https://www.eventstore.com/) and checkpoint back to a store of your choice.

The stream reader is created via. `EventStoreSource.Run` which will return an
`IStreamReader` for either an `AllStreamReader` or a `NamedStreamReader` depending on the
`streamName` parameter.

---

## DISCLAIMER

This library has been plucked from a project I am working and is...

  * Still a work-in-progress
  * Is missing tests
  * Has primarily been tested with reading from the `$all` stream (`AllStreamReader`).
  * May be missing auxiliary components (eg. `paket.dependencies`)

I'm hoping to remedy these points soon, but wanted to share the initial work.

---

## Checkpointing

The checkpointing implementation uses a generic `ICheckpointer` which could use any
backend, but the library also provides an `EventStoreCheckpointer` which implements
this interface.

Obviously, by using the same EventStore instance to read _and_ write from, then
you must be careful not to cause an endless loop. This is where `eventFilter` parameter
comes in.

## Versions

This library is currently using the following pinned dependencies (via. the
[Paket](https://fsprojects.github.io/Paket/) package manager).

```
nuget EventStore.Client ~> 5
nuget protobuf-net == 2.4
```

Once the `Equinox` packages use the new GRPC EventStore client, then I will look
at using it in this package. Until then, this is not possible as the two versions
will not coincide.

## Example

The following example is a simplified example from from the composition root in
my projector:

```fsharp
let buildProjector cfg =
    (*

    `cfg` is just a generic configuration record for the app

    I make a bunch of separately configured `Serilog.ILoggers`, but you
    can use the global logger if you prefer.

        val makeLogger : name:string -> Serilog.ILogger

    *)

    let makeConn connectionName =
        let connector =
            Equinox.EventStore.Connector (
                username         = cfg.Username,
                password         = cfg.Password,
                reqTimeout       = cfg.Timeout,
                reqRetries       = cfg.Retries,
                heartbeatTimeout = cfg.HeartbeatTimeout,
                log              = Logger.SerilogVerbose ((makeLogger "Store").ForContext("source", connectionName)),
                tags             = [ "M", Environment.MachineName; "I", Guid.NewGuid () |> string ]
            )
        connector.Connect (connectionName, cfg.Discovery, cfg.NodePreference)
        |> Async.RunSynchronously

    let sink =
        Propulsion.Streams.StreamsProjector.Start (
            log                  = makeLogger "Projector",
            maxReadAhead         = cfg.MaxReadAhead,
            maxConcurrentStreams = cfg.MaxConcurrentStreams,
            statsInterval        = cfg.StatsInterval,
            handle               = myHandler, // Your stream event handler
            stats                = stats
        )

    let checkpointer =
        EventStoreCheckpointer (
            (makeConn "checkpointer").WriteConnection,
            logger = makeLogger "Checkpointer"
        ) :> ICheckpointer

    let runSourcePipeline =
        EventStoreSource.Run (
            readerLogger      = makeLogger "Reader",
            statsLogger       = makeLogger "Stats",
            conn              = makeConn "source",
            startPosition     = cfg.StartPosition,
            checkpointer      = checkpointer,
            consumerGroup     = cfg.ConsumerGroup,
            maxBatchSize      = cfg.MaxReadAhead,
            sink              = sink,
            eventFilter       = filterEvent, // ResolvedEvent -> bool
            tailSleepInterval = TimeSpan.FromSeconds 5.,
            statsInterval     = cfg.StatsInterval,
            streamType        = cfg.StreamType
        )


[<EntryPoint>]
let main argv =
    // Acquire any configuration
    let cfg = getConfig ()

    let sink, runSourcePipeline = buildProjector cfg

    // Endless loop
    let rec runLoop () = async {
        match! Async.Catch runSourcePipeline with
        | Choice1Of2 _ -> return ()
        | Choice2Of2 ex ->
            logger.Error (ex, "Exception in run loop: {ex}", ex.Message)
            do! Async.Sleep 3_000
            logger.Information ("Restarting run loop")
            return! runLoop ()
    }

    runLoop () |> Async.Start
    sink.AwaitCompletion () |> Async.RunSynchronously
    if sink.RanToCompletion then 0 else 1
```
