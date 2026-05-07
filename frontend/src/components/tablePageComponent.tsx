import type { Dispatch, SetStateAction } from "react";

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
        setRowOffset: Dispatch<SetStateAction<number>>
    }) 
{
    return (
        <>
            <div>
                <button
                    className="mr-1 p-1"
                    onClick={() => setRowOffset(Math.max(rowOffset - rowLimit, 0))}
                > 
                    {"<"}
                </button>
                Page: {Math.floor(rowOffset / rowLimit) + 1} / {Math.ceil(TotNRows / rowLimit)}
                <button
                    className="ml-1 p-1"
                    onClick={() => setRowOffset(Math.min(rowOffset + rowLimit, Math.max(TotNRows - (TotNRows % rowLimit), 0)))}
                >
                    {">"}
                </button>
            </div>
        </>
    )
}
