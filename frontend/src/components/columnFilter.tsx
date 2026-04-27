import { useEffect, useState, useRef, useCallback} from "react"
import { getColumnValues } from "../api/api";
import ColumnObject from "./columnObject";
import type { Props } from "../interfaces/props"


export default function ColumnFilter({ props, tableName }: {props: Props, tableName: string}) {
    const [rowColumns, setRowColumns] = useState<string[]>([]);
    const [pivotColumns, setPivotColumns] = useState<string[]>([]);
    const [columnValuesChoice, setColumnValuesChoice] = useState<Record<string, Record<string, boolean>> | null>(null);
    
    const pivotSectionRef = useRef<HTMLDivElement>(null);
    const columnSectionRef = useRef<HTMLDivElement>(null);
    const colObjectRef = useRef<HTMLDivElement>(null);
    const pivObjectRef = useRef<HTMLDivElement>(null);
    const [positionCol, setPositionCol] = useState({ x: 0, y: 0 });
    const [positionPiv, setPositionPiv] = useState({ x: 0, y: 0 });
    const [draggingCol, setDraggingCol] = useState(false);
    const [draggingPiv, setDraggingPiv] = useState(false);
    const dragStateCol = useRef({ startX: 0, startY: 0 });
    const dragStatePiv = useRef({ startX: 0, startY: 0 });


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

    const onMouseDownCol = useCallback((e: React.MouseEvent) => {
        if (!columnSectionRef.current) return;
        
        setDraggingCol(true);
        const columnSectionRect = columnSectionRef.current.getBoundingClientRect();
        dragStateCol.current = {
            startX: e.clientX - positionCol.x - columnSectionRect.left,
            startY: e.clientY - positionCol.y - columnSectionRect.top,
        }
    }, [positionCol]);

    const onMouseMoveCol = useCallback((e: React.MouseEvent) => {
        if (!draggingCol || !columnSectionRef.current) return;
        
        const columnSectionRect = columnSectionRef.current.getBoundingClientRect();
        setPositionCol({
            x: e.clientX - dragStateCol.current.startX - columnSectionRect.left,
            y: e.clientY - dragStateCol.current.startY - columnSectionRect.top
        });
    }, [draggingCol]);

    const onMouseUpCol = () => {
        setDraggingCol(false);
    }

    const onMouseDownPiv = useCallback((e: React.MouseEvent) => {
        if (!pivotSectionRef.current) return;
        
        setDraggingPiv(true);
        const pivotSectionRect = pivotSectionRef.current.getBoundingClientRect();
        dragStatePiv.current = {
            startX: e.clientX - positionPiv.x - pivotSectionRect.left,
            startY: e.clientY - positionPiv.y - pivotSectionRect.top,
        }
    }, [positionPiv]);

    const onMouseMovePiv = useCallback((e: React.MouseEvent) => {
        if (!draggingPiv || !pivotSectionRef.current) return;
        
        const pivotSectionRect = pivotSectionRef.current.getBoundingClientRect();
        setPositionPiv({
            x: e.clientX - dragStatePiv.current.startX - pivotSectionRect.left,
            y: e.clientY - dragStatePiv.current.startY - pivotSectionRect.top
        });
    }, [draggingPiv]);

    const onMouseUpPiv = () => {
        setDraggingPiv(false);
    }

    function snapToSection(objectRef: React.RefObject<HTMLDivElement>) {

        // snap to section has to happen onMouseUp
        // currently the objects are on top of one other, need to modify onMouseDown to prevent that
        // current snapToSection snaps object one on top of another, give it some clearance by the number of elements in the list

        if (!objectRef.current || !columnSectionRef.current || !pivotSectionRef.current) {
            return undefined;
        }

        const colObjRect = objectRef.current.getBoundingClientRect();
        const colSecRect = columnSectionRef.current.getBoundingClientRect();
        const pivSecRect = pivotSectionRef.current.getBoundingClientRect();

        const distToColSection = (colSecRect.x - colObjRect.x) ** 2 + (colSecRect.y - colObjRect.y) ** 2;
        const distToPivSection = (pivSecRect.x - colObjRect.x) ** 2 + (pivSecRect.y - colObjRect.y) ** 2;

        const closestRect = distToColSection < distToPivSection ? colSecRect : pivSecRect;
        
        return {
            x: closestRect.x,
            y: closestRect.y
        };
    }

    return (
        <>
            <div className="flex">
                <div className="border rounded-md p-1 m-1 relative"
                    ref={columnSectionRef}
                >
                    row Column Section:
                    {rowColumns.map((col) => (
                        <div key={`row-${col}`}
                            ref={colObjectRef}
                            id={`row-${col}`}
                            onMouseDown={onMouseDownCol}
                            onMouseMove={onMouseMoveCol}
                            onMouseUp={onMouseUpCol}
                            style={{
                                position: "absolute",
                                left: `${positionCol.x}px`,
                                top: `${positionCol.y}px`,
                                cursor: draggingCol ? "grabbing" : "grab",
                            }}
                        >
                            {columnValuesChoice && <ColumnObject col={col} values={columnValuesChoice[col]} defaultSelected={true} setColValues={setColumnValues} togglePivotColumn={togglePivotColumn}/>}
                        </div>
                    ))}
                </div>
                <div className="border rounded-md p-1 m-1 relative"
                    ref={pivotSectionRef}
                >
                    pivot Column Section:
                    {pivotColumns.map((col) => (
                        <div key={`pivot-${col}`}
                            ref={pivObjectRef}
                            id={`row-${col}`}
                            onMouseDown={onMouseDownPiv}
                            onMouseMove={onMouseMovePiv}
                            onMouseUp={onMouseUpPiv}
                            style={{
                                position: "absolute",
                                left: `${positionPiv.x}px`,
                                top: `${positionPiv.y}px`,
                                cursor: draggingPiv ? "grabbing" : "grab",
                            }}
                        >
                            {columnValuesChoice && <ColumnObject col={col} values={columnValuesChoice[col]} defaultSelected={false} setColValues={setColumnValues} togglePivotColumn={togglePivotColumn}/>}
                        </div>
                    ))}
                </div>
            </div>            
        </>
    )
}
