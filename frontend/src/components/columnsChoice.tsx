import { useEffect, useState } from "react"
import ColumnUniqueValuesChoice from "./columnValuesChoice";
import type { Dispatch, SetStateAction } from "react";


export default function ColumnsChoice({columns, columnValues, setColumnValuesChoice}: {columns: string[], columnValues: Record<string, string[]>, setColumnValuesChoice: Dispatch<SetStateAction<Record<string, string[]> | null>>}) {
    const [selectedColumns, setSelectedColumns] = useState<Record<string, boolean>>(Object.fromEntries(columns.map(key => [key, true])));

    // eslint-disable-next-line react-hooks/exhaustive-deps
    useEffect(() => {
        setSelectedColumns(columns.reduce((acc, col) => ({ ...acc, [col]: false }), {}))
    }, [columns]);

    useEffect(() => {
        const selectedCols = Object.keys(selectedColumns).filter(key => selectedColumns[key])
        setColumnValuesChoice((prev) => ({
            ...Object.fromEntries(selectedCols.map(col => [col, prev && prev[col] ? prev[col] : []]))
        }))
    }, [selectedColumns]);

    return (
        <>
            <div className="flex flex-wrap gap-2 w-1/2 justify-end">
                {columns.map((col) => (
                    <div key={col} className={`flex p-2 gap-1 rounded-md ${selectedColumns[col] ? "bg-red-500" : ""}`}>
                        <ColumnUniqueValuesChoice col={col} colSelected={selectedColumns[col]} values={columnValues[col]} setColumnValuesChoice={setColumnValuesChoice}/>
                        <button
                            onClick={() => setSelectedColumns((prev) => ({
                                    ...prev,
                                    [col]: !prev[col]
                                })
                            )}
                        > {selectedColumns[col] ? 
                            <div className="border rounded-md p-1">remove</div>: 
                            <div className="border rounded-md p-1">select</div>}
                        </button>
                    </div>
                ))}
            </div>
        </>
    )
}
