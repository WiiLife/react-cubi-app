import TableChoice from "./tableChoice"
import ColumnsChoice from "./columnsChoice"
import type { Props } from "../interfaces/props"


export default function FilterDisplay({props}: {props: Props}) {
    return (
        <>
            <div className="flex justify-between">
                <TableChoice props={props}/>
                <ColumnsChoice props={props}/>
            </div>
        </>
    )
}
