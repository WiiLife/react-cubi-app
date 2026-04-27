import TableChoice from "./tableChoice"
import type { Props } from "../interfaces/props"
import ColumnFilter from "./columnFilter"


export default function FilterDisplay({props}: {props: Props}) {
    return (
        <>
            <div className="flex justify-between">
                <TableChoice props={props}/>
                {props.tableChoice && <ColumnFilter props={props} tableName={props.tableChoice}/>}
            </div>
        </>
    )
}
