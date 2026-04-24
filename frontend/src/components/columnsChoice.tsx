import { useEffect, useState } from "react"
import ColumnUniqueValuesChoice from "./columnValuesChoice";
import type { Props } from "../interfaces/props";
import { getColumnValues } from "../api/api";


export default function ColumnsChoice({props}: {props: Props}) {
    const [columns, setColumns] = useState<string[] | null>(null);
    const [selectedColumns, setSelectedColumns] = useState<Record<string, boolean>>({});
    const [columnValues, setColumnValues] = useState<Record<string, string[]> | null>(null);

    useEffect(() => {
        async function fetchColumnValues(table: string) {
            const columnValues = await getColumnValues(table)
            setColumnValues(columnValues)
            setColumns(Object.keys(columnValues))
            setSelectedColumns(Object.fromEntries(Object.keys(columnValues).map(key => [key, true])) as Record<string, boolean>)
        }   
        if (props.tableChoice) {
            fetchColumnValues(props.tableChoice) 
        }
    }, [props.tableChoice])

    return (
        <>
            <div className="flex flex-wrap gap-2 w-1/2 justify-end">
                {columnValues && selectedColumns && columns?.map((col) => (
                    <div key={col} className={`flex p-2 gap-1 rounded-md ${selectedColumns[col] ? "bg-red-500" : ""}`}>
                        <ColumnUniqueValuesChoice props={props} col={col} colSelected={selectedColumns[col]} setColSelected={setSelectedColumns} colValues={columnValues}/>
                        <button
                            onClick={() => setSelectedColumns((prev) => ({
                                    ...prev,
                                    [col]: false
                                })
                            )}
                        > 
                            {selectedColumns[col] && <div className="border rounded-md p-1">remove</div>}
                        </button>
                    </div>
                ))}
            </div>
        </>
    )
}
