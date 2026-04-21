export interface Props {
    tableNames: {"tables": string[]}
    setTableChoice: (table: string) => void
    columns: string[] | null
    setColumnsChoice: (cols: string[]) => void
}
