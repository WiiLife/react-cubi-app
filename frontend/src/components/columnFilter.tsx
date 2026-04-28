import { useEffect, useState, useRef} from "react"
import { getColumnValues } from "../api/api";
import ColumnObject from "./columnObject";
import type { Props } from "../interfaces/props"
import GrapComponent from "./grabComponent";


export default function ColumnFilter({ props, tableName }: {props: Props, tableName: string}) {
    const [rowColumns, setRowColumns] = useState<string[]>([]);
    const [pivotColumns, setPivotColumns] = useState<string[]>([]);
    const [columnValuesChoice, setColumnValuesChoice] = useState<Record<string, Record<string, boolean>> | null>(null);
    
    const pivotSectionRef = useRef<HTMLDivElement | null>(null);
    const columnSectionRef = useRef<HTMLDivElement | null>(null);
    const fullSectionRef = useRef<HTMLDivElement | null>(null);

    useEffect(() => {
        async function fetchColumnValues(tableName: string) {
            const res = await getColumnValues(tableName);

            // set the inital column-values choice object
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

            // set the initial rowColumns
            setRowColumns(Object.keys(res))

            // set initial pivotColumns
            setPivotColumns([])
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
            return {
                ...prev,
                [col]: {
                    ...prevColValues,
                    [value]: !(prevColValues[value] ?? false)
                }
            };
        });
    }

    function togglePivotColumn( col: string, method: "toggle" | "pivot" | "row" ) {
        if (method === "toggle") {
            if (!pivotColumns.includes(col)) {
                setRowColumns((prev) => prev.filter((rowCol) => rowCol !== col));
                setPivotColumns((prev) => [...prev, col]);
            } else {
                setPivotColumns((prev) => prev.filter((pivotCol) => pivotCol !== col));
                setRowColumns((prev) => [...prev, col]);
            }
        } if (method == "pivot") {
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
                className="flex min-h-80 relative"
                ref={fullSectionRef}
            >
                <div 
                    className="border rounded-md p-1 m-1"
                    ref={columnSectionRef}
                >
                    row Column Section:
                    {columnValuesChoice && rowColumns.map((col) => (
                        <div key={`row-${col}`}>
                            <GrapComponent containerRef={fullSectionRef} colContainerRef={columnSectionRef} pivContainerRef={pivotSectionRef}>
                                <ColumnObject col={col} values={columnValuesChoice[col]} defaultSelected={true} setColValues={setColumnValues} togglePivotColumn={togglePivotColumn}/>
                            </GrapComponent>
                        </div>
                    ))}
                </div>
                <div className="border rounded-md p-1 m-1"
                    ref={pivotSectionRef}
                >
                    pivot Column Section:
                    {columnValuesChoice && pivotColumns.map((col) => (
                        <div key={`pivot-${col}`}>
                            <GrapComponent containerRef={fullSectionRef} colContainerRef={columnSectionRef} pivContainerRef={pivotSectionRef}>
                                <ColumnObject col={col} values={columnValuesChoice[col]} defaultSelected={false} setColValues={setColumnValues} togglePivotColumn={togglePivotColumn}/>
                            </GrapComponent>
                        </div>
                    ))}
                </div>
            </div>            
        </>
    )
}
