import type { Dispatch, SetStateAction } from "react"

export interface Props {
    tableChoice: string | null
    setTableChoice: (table: string) => void
    pivotColumns: string[] | null
    setPivotColumns: Dispatch<SetStateAction<string[]>>
    columnValuesChoice: Record<string, string[]> | null
    setColumnValuesChoice: Dispatch<SetStateAction<Record<string, string[]> | null>>
}

