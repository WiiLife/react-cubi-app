import { useEffect, useState } from "react"
import ColumnUniqueValuesChoice from "./columnValuesChoice";


export default function ColumnsChoice({columns, setColumnsChoice}: {columns: string[], setColumnsChoice: (cols: string[]) => void}) {
    const [selectedColumns, setSelectedColumns] = useState<Record<string, boolean>>({});

    // eslint-disable-next-line react-hooks/exhaustive-deps
    useEffect(() => {
        setSelectedColumns(columns.reduce((acc, col) => ({ ...acc, [col]: true }), {}))
    }, [columns]);

    useEffect(() => {
        const selectedCols = Object.keys(selectedColumns).filter(key => selectedColumns[key])
        setColumnsChoice(selectedCols)
        console.log(Object.keys(selectedColumns).filter(key => selectedColumns[key]))
    }, [selectedColumns]);

    return (
        <>
            <div className="flex flex-wrap gap-2 w-1/2 justify-end">
                {columns.map((col) => (
                    <div key={col} className={`flex p-2 gap-1 rounded-md ${selectedColumns[col] ? "bg-red-500" : ""}`}>
                        <ColumnUniqueValuesChoice col={col}/>
                        <button
                            onClick={() => selectedColumns[col] ? 
                                setSelectedColumns((prev) => ({
                                    ...prev,
                                    [col]: false
                                }))
                                : setSelectedColumns((prev) => ({
                                    ...prev,
                                    [col]: true
                                })
                            )}
                        > {selectedColumns[col] ? 
                            <div className="border rounded-md p-1">x</div>: 
                            <div className="border rounded-md p-1">select</div>}
                        </button>
                    </div>
                ))}
            </div>
        </>
    )
}
