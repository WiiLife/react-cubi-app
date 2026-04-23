import type { Dispatch, SetStateAction } from "react"

export interface Props {
    tableNames: string[] | null
    tableChoice: string | null
    setTableChoice: (table: string) => void
    columns: string[] | null
    columnValues: Record<string, string[]> | null
    columnValuesChoice: Record<string, string[]> | null
    setColumnValuesChoice: Dispatch<SetStateAction<Record<string, string[]> | null>>
}

