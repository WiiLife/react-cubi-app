import { useEffect, useState, useRef } from "react"
import TablePageComponent from "./tablePageComponent";
import { getTable } from "../api/api"


export default function TableCard({table, columnValues, pivotColumns}: {table: string, columnValues: Record<string, string[]>, pivotColumns: string[]}) {
    const [tableData, setTableData] = useState<Record<string, unknown>[] | null>(null);
    const [columns, setColumns] = useState<string[] | null>(null);
    const prevColumnsRef = useRef<Record<string, string[]> | null>(null);
    const prevPivotRef = useRef<string[] | null>(null);
    const [rowLimit,] = useState<number>(20);
    const [rowOffset, setRowOffset] = useState<number>(0);
    const [nRows, setNRows] = useState<number>(0);

    useEffect(() => {
        async function fetchTable() {
            const filteredColumns = Object.fromEntries(
                Object.entries(columnValues).filter(([, vals]) => vals.length > 0)
            ) as Record<string, string[]>
            setTableData(null);
            setColumns(Object.keys(columnValues))
            if (Object.entries(filteredColumns).length > 0) {
                const res = await getTable({
                    table,
                    columns: filteredColumns,
                    pivot_cols: pivotColumns,
                    rowLimit,
                    rowOffset
                })
                setTableData(res.data)
                setColumns(Object.keys(res.data[0]))
                setNRows(res.n_rows)
            }
        };

        if (prevColumnsRef.current !== columnValues || prevPivotRef.current !== pivotColumns) {
            fetchTable()
            prevColumnsRef.current = columnValues;
            prevPivotRef.current = pivotColumns
        }
    }, [table, columnValues, pivotColumns, rowLimit, rowOffset])

    return (
        <>
            <div>
                {columns && <div className="p-4 overflow-x-auto">
                    <div>
                        <table className="w-full border-collapse border border-gray-300">
                            <thead>
                                <tr className="bg-gray-100 dark:bg-gray-800">
                                    {columns.map((col) => (
                                        <th key={col} className="border border-gray-300 px-4 py-2 text-left">
                                            {col}
                                        </th>
                                    ))}
                                </tr>
                            </thead>
                            <tbody>
                                {tableData?.map((record, recordIndex) => (
                                    <tr key={recordIndex} className="hover:bg-gray-50 dark:hover:bg-gray-900">
                                        {columns.map((col, index) => (
                                            <td key={`${recordIndex}_${index}`} className="border border-gray-300 px-4 py-2">
                                                {String(record[col])}
                                            </td>
                                        ))}
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                        <div className="flex justify-end m-1">
                            <TablePageComponent 
                                TotNRows={nRows} 
                                rowLimit={rowLimit} 
                                rowOffset={rowOffset} 
                                setRowOffset={setRowOffset}
                            />
                        </div>
                    </div>
                </div>}
            </div>
        </>
    )
}
