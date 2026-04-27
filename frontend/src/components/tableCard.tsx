import { useEffect, useState } from "react"
import { getTable } from "../api/api"


export default function TableCard({table, columnValues, pivotColumns}: {table: string, columnValues: Record<string, string[]>, pivotColumns: string[]}) {
    const [tableData, setTableData] = useState<Record<string, unknown>[] | null>(null);
    const [columns, setColumns] = useState<string[] | null>(null);

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
                    pivot_cols: pivotColumns
                })
                setTableData(res)
                setColumns(Object.keys(res[0]))
            }
        };

        fetchTable();
    }, [table, columnValues, pivotColumns])

    return (
        <>
            <div>
                {columns && <div className="p-4">
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
                </div>}
            </div>
        </>
    )
}
