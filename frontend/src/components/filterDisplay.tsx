import TableChoice from "./tableChoice"
import ColumnsChoice from "./columnsChoice"
import type { Props } from "../interfaces/props"


export default function FilterDisplay({props}: {props: Props}) {
    return (
        <>
            <div className="flex justify-between">
                <TableChoice tableNames={props.tableNames} setTableChoice={props.setTableChoice}/>
                {props.columns && props.columnValues && <ColumnsChoice columns={props.columns} columnValues={props.columnValues} setColumnValuesChoice={props.setColumnValuesChoice}/>}
            </div>
        </>
    )
}
