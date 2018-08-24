open System
open System.Linq
open felt


let CreateOneExamplePipe jobContext index =
    seq { 0 .. index }
    |> Source.getWithOptions (SourceOptions.CreateDefault(rowLabel = "CreateSource")) jobContext
    |> Source.mapWithOptions (SourceOptions.CreateDefault(rowLabel = "ConvertToString")) (fun row ->
        row.Value.ToString()
    ) |> Source.whereWithOptions (SourceOptions.CreateDefault(rowLabel = "FilterOdd")) (fun row ->
        row.Index % 2 = 0
    )

let NotifySlack =
    Source.iterWithOptions (SourceOptions.CreateDefault(rowLabel = "NotifySlack")) (fun row ->
        row.Value
        |> printfn "Fix Yo Stuff Yo  %s."
    )

let EmailIfProblem pipeline =
    pipeline
    |> Source.mapWithOptions (SourceOptions.CreateDefault(rowLabel = "MultiplyString")) (fun row ->
        row.Value
        |> Int32.Parse
        |> (*) 2
        |> sprintf "%i"
    )
    |> Source.iterWithOptions (SourceOptions.CreateDefault(rowLabel = "EmailProblem")) (fun row ->
        row.Value
        |> printfn "Here's an email  %s."
    )


let LogPipeLineRejects pipeline =
    pipeline
    |> Source.getRejectsWithOptions (SourceOptions.CreateDefault(rowLabel = "GetRejects"))
    |> EmailIfProblem

[<EntryPoint>]
let main argv =
    let jobContext =
        {
            JobContext.CreateDefault()
            with
                Name = "MainJob"
                JobOptions = { JobOptions.Default with EmptyRows = false  }
        }


    let examplePipe1 = CreateOneExamplePipe jobContext 7


    [|  examplePipe1 |> NotifySlack
        examplePipe1 |> EmailIfProblem
        examplePipe1 |> LogPipeLineRejects

    |]
    |> Source.evaluate


    printfn ""
    printfn "ID;Label;Count"
    for item in jobContext.Counters do
        printfn "%s;%s;%i" item.Key item.Value.Label item.Value.RowCount

    printfn ""
    printfn "digraph DataFlow {"
    for options in jobContext.SourceOptions.Values do
        for dependency in options.Dependencies.Values do
            let dependencyLabel = if options.SourceType = SourceType.GetRejects then dependency.RejectRowLabel else dependency.RowLabel
            printfn "    \"%s - %A\" -> \"%s - %A\"" dependencyLabel dependency.SourceType options.RowLabel options.SourceType
            if options.SourceType = SourceType.Where then
                printfn "    \"%s - %A\" -> \"%s - %A\"" dependencyLabel dependency.SourceType options.RejectRowLabel options.SourceType
    printfn "}"

    printfn ""
    printfn "digraph Nodes {"
    for options in jobContext.SourceOptions.Values do
        for dependency in options.Dependencies.Values do
            printfn "    \"%s - %A\" -> \"%s - %A\"" dependency.RowLabel dependency.SourceType options.RowLabel options.SourceType
    printfn "}"


    printfn ""
    printfn "digraph DataFlow {"
    for options in jobContext.SourceOptions.Values do
        for dependency in options.Dependencies.Values do
            let dependencyLabel = if options.SourceType = SourceType.GetRejects then dependency.RejectRowLabel else dependency.RowLabel

            let count =
                options.ID
                |> jobContext.GetCounterById
                |> Option.map(fun counter ->
                    sprintf "[label=\"%i\"]" counter.RowCount
                )
            let countLabel = defaultArg count String.Empty

            printfn "    \"%s - %s - %A\" -> \"%s - %s - %A\" %s" dependency.ID dependencyLabel dependency.SourceType options.ID options.RowLabel options.SourceType countLabel
            if options.SourceType = SourceType.Where then

                let count =
                    options.RejectID
                    |> jobContext.GetCounterById
                    |> Option.map(fun counter ->
                        sprintf "[label=\"%i\"]" counter.RowCount
                    )
                let countLabel = defaultArg count String.Empty

                printfn "    \"%s - %s - %A\" -> \"%s - %s - %A\" %s" dependency.ID dependencyLabel dependency.SourceType options.ID options.RejectRowLabel options.SourceType countLabel
    printfn "}"

    printfn ""
    printfn "digraph Nodes {"
    for options in jobContext.SourceOptions.Values do
        for dependency in options.Dependencies.Values do
            printfn "    \"%s - %s - %A\" -> \"%s - %s - %A\"" dependency.ID dependency.RowLabel dependency.SourceType options.ID options.RowLabel options.SourceType
    printfn "}"

    0 // return an integer exit code
