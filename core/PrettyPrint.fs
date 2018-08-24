module felt.PrettyPrint

open System

let DataFlow (jobContext: JobContext) =
    let builder = new System.Text.StringBuilder()

    builder.Append "digraph DataFlow {"
    |> ignore

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

            sprintf "    \"%s - %s - %A\" -> \"%s - %s - %A\" %s" dependency.ID dependencyLabel dependency.SourceType options.ID options.RowLabel options.SourceType countLabel
            |> builder.AppendLine
            |> ignore

            if options.SourceType = SourceType.Where then

                let count =
                    options.RejectID
                    |> jobContext.GetCounterById
                    |> Option.map(fun counter ->
                        sprintf "[label=\"%i\"]" counter.RowCount
                    )
                let countLabel = defaultArg count String.Empty

                sprintf "    \"%s - %s - %A\" -> \"%s - %s - %A\" %s" dependency.ID dependencyLabel dependency.SourceType options.ID options.RejectRowLabel options.SourceType countLabel
                |> builder.AppendLine
                |> ignore

    builder.AppendLine "}"
    |> ignore

    builder.ToString()


let Nodes (jobContext: JobContext) =
    let builder = new System.Text.StringBuilder()
    "digraph Nodes {"
    |> builder.AppendLine
    |> ignore

    for options in jobContext.SourceOptions.Values do
        for dependency in options.Dependencies.Values do
            sprintf "    \"%s - %s - %A\" -> \"%s - %s - %A\"" dependency.ID dependency.RowLabel dependency.SourceType options.ID options.RowLabel options.SourceType
            |> builder.AppendLine
            |> ignore

    builder.AppendLine "}"
    |> ignore

    builder.ToString()
