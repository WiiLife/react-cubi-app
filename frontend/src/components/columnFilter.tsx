import { useEffect, useState, useRef} from "react"
import { getColumnValues } from "../api/api";
import ColumnObject from "./columnObject";
import type { Props } from "../interfaces/props"
import GrapComponent from "./grabComponent";


export default function ColumnFilter({ props, tableName }: {props: Props, tableName: string}) {
    const [rowColumns, setRowColumns] = useState<string[]>([]);
    const [pivotColumns, setPivotColumns] = useState<string[]>([]);
    const [columnValuesChoice, setColumnValuesChoice] = useState<Record<string, Record<string, boolean>> | null>(null);
    const [cantRemoveCol, setCantRemoveCol] = useState<string | null>(null);

    const pivotSectionRef = useRef<HTMLDivElement | null>(null);
    const columnSectionRef = useRef<HTMLDivElement | null>(null);
    const fullSectionRef = useRef<HTMLDivElement | null>(null);

    useEffect(() => {
        async function fetchColumnValues(tableName: string) {
            const res = await getColumnValues(tableName);

            setRowColumns(Object.keys(res))
            setPivotColumns([])
            setColumnValuesChoice(
                Object.fromEntries(
                    Object.entries(res).map(([col, values]: [string, string[]]) => [
                        col,
                        Object.fromEntries(
                            values.map((val: string, index: number) => [val, index === 0])
                        )
                    ])
                )
            )
        }

        fetchColumnValues(tableName)        
    }, [tableName])

    useEffect(() => {
        if (!columnValuesChoice) return;
        props.setColumnValuesChoice(Object.fromEntries(
            Object.entries(columnValuesChoice)
            .map(([col, record]) => [col,
                Object.entries(record).filter(([, selected]) => selected).map(([val]) => val)
            ])
            .filter(([, values]) => values.length > 0)
        ))

        if (!pivotColumns) return;
        props.setPivotColumns(pivotColumns)

    }, [columnValuesChoice, rowColumns, pivotColumns])

    function setColumnValues(col: string, value: string) {
        
        
        setColumnValuesChoice((prev) => {
            if (!prev) return prev;

            const prevColValues = prev[col] ?? {};
            const toNotRemove = (cantRemoveCol === col) && (Object.entries(prevColValues).filter((value) => value[1]).length == 1)
            return {
                ...prev,
                [col]: {
                    ...prevColValues,
                    [value]: toNotRemove ? true : !(prevColValues[value] ?? false)
                }
            };
        });
    }

    function togglePivotColumn( col: string, method: "toggle" | "pivot" | "row" ) {
        setCantRemoveCol(null);
        if (method === "toggle") {
            if (!pivotColumns.includes(col)) {
                if (rowColumns.length == 1) {setCantRemoveCol(rowColumns[0]); return;}
                setRowColumns((prev) => prev.filter((rowCol) => rowCol !== col));
                setPivotColumns((prev) => [...prev, col]);
            } else {
                setPivotColumns((prev) => prev.filter((pivotCol) => pivotCol !== col));
                setRowColumns((prev) => [...prev, col]);

                if (!columnValuesChoice || Object.entries(columnValuesChoice[col]).filter(([, selected]) => selected).length > 0) return
                setColumnValues(col, Object.entries(columnValuesChoice[col]).filter((_, index) => index == 0)[0][0])

            }
        } if (method == "pivot") {
            if (rowColumns.length == 1) {setCantRemoveCol(rowColumns[0]); return;}
            if (!pivotColumns.includes(col)) {
                setRowColumns((prev) => prev.filter((rowCol) => rowCol !== col));
                setPivotColumns((prev) => [...prev, col]);
            }
        } if (method == "row") {
            if (pivotColumns.includes(col)) {
                setPivotColumns((prev) => prev.filter((pivotCol) => pivotCol !== col));
                setRowColumns((prev) => [...prev, col]);                
            }
        }
    }    

    return (
        <>
            <div 
                className="flex min-h-96 min-w-2xl"
                ref={fullSectionRef}
            >
                
                <div className="flex-col min-w-1/2">
                    <div>Rows</div>
                    <div 
                        className="border rounded-md p-1 w-full h-full"
                        ref={columnSectionRef}
                    >
                        {columnValuesChoice && rowColumns.map((col, index) => (
                            <div key={`row-${col}`}>
                                <GrapComponent col={col} containerRef={columnSectionRef} otherRef={pivotSectionRef} index={index} togglePivotColumn={togglePivotColumn}>
                                    <ColumnObject col={col} values={columnValuesChoice[col]} cantRemoveCol={cantRemoveCol} defaultSelected={true} setColValues={setColumnValues} togglePivotColumn={togglePivotColumn} />
                                </GrapComponent>
                            </div>
                        ))}
                    </div>
                </div>
                
                <div className="flex-col min-w-1/2">
                    <div>Columns</div>
                    <div className="border rounded-md p-1 w-full h-full"
                        ref={pivotSectionRef}
                    >
                        {columnValuesChoice && pivotColumns.map((col, index) => (
                            <div key={`pivot-${col}`}>
                                <GrapComponent col={col} containerRef={pivotSectionRef} otherRef={columnSectionRef} index={index} togglePivotColumn={togglePivotColumn}>
                                    <ColumnObject col={col} values={columnValuesChoice[col]} cantRemoveCol={cantRemoveCol} defaultSelected={false} setColValues={setColumnValues} togglePivotColumn={togglePivotColumn}/>
                                </GrapComponent>
                            </div>
                        ))}
                    </div>
                </div>
                
            </div>            
        </>
    )
}
