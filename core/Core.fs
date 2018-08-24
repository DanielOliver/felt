namespace felt

open System
open System.Linq

type RowCounter(label: string) =
    let mutable rowCount = 0
    member this.Increment() = rowCount <- rowCount + 1
    member this.RowCount with get () = rowCount
    member this.GetCount() = rowCount
    member this.Label = label

type JobOptions =
    {   EmptyRows: bool
    }
    static member Default =
        {   EmptyRows = false
        }

type JobContext =
    {   ID: string
        Name: string
        Counters: System.Collections.Generic.IDictionary<string, RowCounter>
        JobOptions: JobOptions
        SourceOptions: System.Collections.Generic.IDictionary<string, SourceOptions>
    }
    static member CreateDefault() =
        {
            ID = System.Guid.NewGuid().ToString()
            Name = System.Guid.NewGuid().ToString()
            Counters = new System.Collections.Generic.Dictionary<string, RowCounter>()
            SourceOptions = new System.Collections.Generic.Dictionary<string, SourceOptions>()
            JobOptions = { JobOptions.Default with EmptyRows = false  }
        }

    member this.GetCounter id name =
        match this.Counters.TryGetValue id with
        | true, counter -> counter
        | false, _ ->
            let counter = RowCounter(name)
            this.Counters.[id] <- counter
            counter
    member this.GetCounterById id =
        match this.Counters.TryGetValue id with
        | true, counter -> Some counter
        | false, _ -> None

    member this.AddSourceOptions (options: SourceOptions) =
        this.SourceOptions.[options.ID] <- options

and Source<'T> =
    interface
        abstract member Rows: seq<'T>
        abstract member ProcessedRowCount: unit -> int
        abstract member JobContext: JobContext
        abstract member Options: SourceOptions
    end

and RejectSource<'T> =
    interface
        abstract member RejectRows: seq<'T>
        abstract member RejectRowCount: unit -> int
    end

and WhereSource<'T> =
    interface
        inherit RejectSource<'T>
        inherit Source<'T>
    end

and SourceOptions =
    {   RowLabel: string
        RejectRowLabel: string
        ID: string
        SourceType: SourceType
        Dependencies: System.Collections.Generic.IDictionary<string, SourceOptions>
    }
    static member CreateDefault(?rowLabel, ?rejectRowLabel) =
        let guid = (System.Guid.NewGuid().ToString().Replace("-",""))
        let rowLabel = defaultArg rowLabel guid
        {   RowLabel = rowLabel
            RejectRowLabel = defaultArg rejectRowLabel (rowLabel + "Reject")
            ID = guid
            SourceType = SourceType.Undefined
            Dependencies = new System.Collections.Generic.Dictionary<string, SourceOptions>()
        }
    member this.RejectID = this.ID + "Reject"
    member this.ReplaceUndefined sourceType =
        if this.SourceType = SourceType.Undefined then
            { this with SourceType = sourceType }
        else this
    member this.AddDependency (options: SourceOptions) =
        this.Dependencies.[options.ID] <- options

and [<RequireQualifiedAccess>] SourceType =
    | Undefined
    | Get
    | Map
    | Where
    | GetRejects
    | Iter
    | IterAnd

[<Struct>]
type RowContext<'T> =
    {   Value: 'T
        Index: int
        JobContext: JobContext
        SourceOptions: SourceOptions
    }

module Source =

    let getWithOptions (options: SourceOptions) (jobContext: JobContext) (rows: seq<'T>): Source<'T> =
        let options = options.ReplaceUndefined SourceType.Get
        jobContext.AddSourceOptions options
        let counter = jobContext.GetCounter options.ID options.RowLabel
        let fieldedSource =
            (if jobContext.JobOptions.EmptyRows then Seq.empty else rows)
            |> Seq.mapi(fun index item ->
                counter.Increment()
                item)
            |> Seq.cache
        { new Source<_> with
            member this.JobContext = jobContext
            member this.Rows = fieldedSource
            member this.ProcessedRowCount() = counter.GetCount()
            member this.Options = options
        }

    let get (context: JobContext) (rows: seq<'T>): Source<'T> =
        getWithOptions (SourceOptions.CreateDefault()) context rows

    let mapWithOptions (options: SourceOptions) (mapper: RowContext<'T> -> 'U) (source: Source<'T>): Source<'U> =
        let options = options.ReplaceUndefined SourceType.Map
        source.JobContext.AddSourceOptions options
        options.AddDependency source.Options
        let counter = source.JobContext.GetCounter options.ID options.RowLabel
        let fieldedSource =
            source.Rows
            |> Seq.mapi(fun index item ->
                counter.Increment()
                mapper
                    {   Value = item
                        Index = index
                        JobContext = source.JobContext
                        SourceOptions = options
                    })
            |> Seq.cache
        { new Source<_> with
            member this.JobContext = source.JobContext
            member this.Rows = fieldedSource
            member this.ProcessedRowCount() = counter.GetCount()
            member this.Options = options
        }

    let map (mapper: RowContext<'T> -> 'U) (source: Source<'T>): Source<'U> =
        mapWithOptions (SourceOptions.CreateDefault()) mapper source

    let getRejectsWithOptions (options: SourceOptions) (rejectSource: 'U when 'U :> RejectSource<'T> and 'U :> Source<'T>): Source<'T> =
        let options = options.ReplaceUndefined SourceType.GetRejects
        rejectSource.JobContext.AddSourceOptions options
        options.AddDependency rejectSource.Options
        let counter = rejectSource.JobContext.GetCounter options.ID options.RowLabel
        let fieldedSource =
            rejectSource.RejectRows
            |> Seq.mapi(fun index item ->
                counter.Increment()
                item)
            |> Seq.cache
        { new Source<_> with
            member this.JobContext = rejectSource.JobContext
            member this.Rows = fieldedSource
            member this.ProcessedRowCount() = counter.GetCount()
            member this.Options = options
        }

    let getRejects (rejectSource): Source<'T> =
        getRejectsWithOptions (SourceOptions.CreateDefault()) rejectSource

    let whereWithOptions (options: SourceOptions) (filter: RowContext<'T> -> bool) (source: Source<'T>): WhereSource<'T> =
        let options = options.ReplaceUndefined SourceType.Where
        source.JobContext.AddSourceOptions options
        options.AddDependency source.Options
        let counter = source.JobContext.GetCounter options.ID options.RowLabel
        let rejectCounter = source.JobContext.GetCounter (options.RejectID) options.RejectRowLabel
        let fieldedSource =
            source.Rows
            |> Seq.mapi(fun index item ->
                let row = {   Value = item
                              Index = index
                              JobContext = source.JobContext
                              SourceOptions = options
                          }
                let shouldAccept =
                    {   Value = item
                        Index = index
                        JobContext = source.JobContext
                        SourceOptions = options
                    } |> filter
                if shouldAccept then counter.Increment()
                else rejectCounter.Increment()
                shouldAccept, row.Value)
            |> Seq.cache
        let acceptSource = fieldedSource |> Seq.where(fun (shouldAccept, _) -> shouldAccept) |> Seq.map (fun (_, item) -> item)
        let rejectSource = fieldedSource |> Seq.where(fun (shouldAccept, _) -> shouldAccept |> not) |> Seq.map (fun (_, item) -> item)
        { new WhereSource<_> with
            member this.RejectRows = rejectSource
            member this.RejectRowCount() = rejectCounter.GetCount()
            member this.JobContext = source.JobContext
            member this.Rows = acceptSource
            member this.ProcessedRowCount() = counter.GetCount()
            member this.Options = options
        }

    let where (filter: RowContext<'T> -> bool) (source: Source<'T>): WhereSource<'T> =
        whereWithOptions (SourceOptions.CreateDefault()) filter source

    let iterAndWithOptions (options: SourceOptions) (action: RowContext<'T> -> unit) (source: Source<'T>): Source<'T> =
        let options = options.ReplaceUndefined SourceType.IterAnd
        source.JobContext.AddSourceOptions options
        options.AddDependency source.Options
        source
        |> mapWithOptions options (fun row ->
            action row
            row.Value
        )

    let iterAnd (action: RowContext<'T> -> unit) (source: Source<'T>): Source<'T> =
        iterAndWithOptions (SourceOptions.CreateDefault()) action source

    let iterWithOptions (options: SourceOptions) (action: RowContext<'T> -> unit) (source: Source<'T>) =
        let options = options.ReplaceUndefined SourceType.Iter
        source.JobContext.AddSourceOptions options
        options.AddDependency source.Options
        source
        |> mapWithOptions options (fun row ->
            action row
        )

    let iter (action: RowContext<'T> -> unit) (source: Source<'T>) =
        iterWithOptions (SourceOptions.CreateDefault()) action source

    let toSeq (source: Source<'T>) =
        source.Rows

    let toList (source: Source<'T>) =
        source.Rows
        |> Seq.toList

    let evaluate (sources: Source<unit> array) =
        sources
        |> Array.iter(fun t ->
            t.Rows
            |> Seq.iteri(fun index item -> ())
        )
