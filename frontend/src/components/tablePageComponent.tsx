export default function TablePageComponent(
    {
        TotNRows, 
        rowLimit, 
        rowOffset, 
        setRowOffset
    }: 
    {
        TotNRows: number, 
        rowLimit: number,
        rowOffset: number,
        setRowOffset: (rowOffset: number) => void
    }) 
{

    console.log("Page: ", Math.floor(rowOffset / rowLimit) + 1)
    console.log("Total Pages: ", Math.ceil(TotNRows / rowLimit))
    console.log(`row offset: ${rowOffset}`)

    return (
        <>
            <button
                className="mr-1 bg-green-600"
                onClick={() => setRowOffset(0)}
            >
                {"<"}
            </button>
            Page: {Math.floor(rowOffset / rowLimit) + 1} / {Math.ceil(TotNRows / rowLimit)}
            <button
                className="ml-1 bg-green-600"
                onClick={() => setRowOffset(rowOffset + rowOffset)}
            >
                {">"}
            </button>
        </>
    )
}
