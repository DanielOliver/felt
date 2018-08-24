module felt.Tests

open System
open System.Collections.Generic
open Xunit
open felt


let getDefaultContext() =
    JobContext.CreateDefault()

let private CreateMapPipeline pipeline =
    pipeline
    |> Source.mapWithOptions (SourceOptions.CreateDefault(rowLabel = "MultiplyBy2")) (fun row ->
        row.Value * 2
    )


let private CreateFilterPipeline pipeline =
    pipeline
    |> Source.whereWithOptions (SourceOptions.CreateDefault(rowLabel = "OnlyEvenNumbers")) (fun row ->
        row.Value % 2 = 0
    )


[<Fact>]
let ``Test Map PipeLine`` () =
    let jobContext = getDefaultContext()
    let source =
        seq { 0..5 }
        |> Source.get jobContext
        |> CreateMapPipeline
    Assert.Equal(0, source.ProcessedRowCount())
    let values =
        source
        |> Source.toList
    let expected = [0; 2; 4; 6; 8; 10]
    Assert.Equal<IEnumerable<int>>(expected, values)
    Assert.Equal(6, source.ProcessedRowCount())

[<Fact>]
let ``Test Filter PipeLine`` () =
    let jobContext = getDefaultContext()
    let source =
        seq { 0..5 }
        |> Source.get jobContext
        |> CreateFilterPipeline
    Assert.Equal(0, source.ProcessedRowCount())
    let values =
        source
        |> Source.toList
    let expected = [0; 2; 4]
    Assert.Equal<IEnumerable<int>>(expected, values)
    Assert.Equal(3, source.ProcessedRowCount())
