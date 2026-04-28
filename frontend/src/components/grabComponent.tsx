import { useRef, useState, useCallback, useLayoutEffect } from "react";
import type { MouseEvent, ReactNode, RefObject } from "react";

type GrapComponentProps = {
    col: string
    containerRef: RefObject<HTMLDivElement | null>;
    otherRef: RefObject<HTMLDivElement | null>;
    children: ReactNode;
    index: number
    togglePivotColumn: (col: string, method: "toggle" | "pivot" | "row") => void
};

export default function GrapComponent({ col, containerRef, otherRef, children, index, togglePivotColumn }: GrapComponentProps) {
    const grabElementRef = useRef<HTMLDivElement>(null);
    const [grabElementPos, setGrabElementPos] = useState<{x: number, y: number}>({x: 0, y: 0});
    const [dragging, setDragging] = useState<boolean>(false);

    useLayoutEffect(() => {
        if (!grabElementRef.current || !containerRef.current) return
        const elemRect = grabElementRef.current.getBoundingClientRect()
        const containerRect = containerRef.current.getBoundingClientRect()
        const elemSize = {w: elemRect.width, h: elemRect.height}

        setGrabElementPos({
            x: containerRect.left,
            y: containerRect.top + (elemSize.h * index)
        })

    }, [index, containerRef])

    const getCenterOfElement = (elemRef: React.RefObject<HTMLDivElement | null>, relative: boolean) => {
        if (!elemRef.current) return { x: 0, y: 0 }
        const elemRect = elemRef.current.getBoundingClientRect()

        if (relative) {
            const elemSize = {w: elemRect.width, h: elemRect.height}
            return ({
                x: elemSize.w / 2,
                y: elemSize.h / 2
            })
        } else {
            return ({
                x: elemRect.x - elemRect.width / 2,
                y: elemRect.y - elemRect.height / 2
            })
        }
    }

    const onMouseDown = useCallback(() => {
        if (!grabElementRef.current || !containerRef.current) return
        setDragging(true);
    }, [containerRef])

    const onMouseMove = useCallback((e: MouseEvent<HTMLDivElement>) => {
        if (!dragging || !containerRef.current) return

        const centerElem = getCenterOfElement(grabElementRef, true)
        setGrabElementPos({
            x: e.clientX - centerElem.x,
            y: e.clientY - centerElem.y
        })

    }, [dragging, containerRef])

    const onMouseUp = () => {
        if (!grabElementRef.current || !containerRef.current) return
        setDragging(false)

        const containerRect = containerRef.current.getBoundingClientRect()
        
        const containerCenter = getCenterOfElement(containerRef, false);
        const otherCenter = getCenterOfElement(otherRef, false);
        const elmCenter = getCenterOfElement(grabElementRef, false);

        const elemRect = grabElementRef.current.getBoundingClientRect()
        const elemSize = {w: elemRect.width, h: elemRect.height}

        if (
            ((containerCenter.x - elmCenter.x) ** 2 + (containerCenter.y - elmCenter.y) ** 2) < 
            ((otherCenter.x - elmCenter.x) ** 2 + (otherCenter.y - elmCenter.y) ** 2)
        ) {
            togglePivotColumn(col, "row")
        } else {
            togglePivotColumn(col, "pivot")
        }

        setGrabElementPos({
            x: containerRect.left,
            y: containerRect.top + (elemSize.h * index)
        })
    }

    return (
        <>
            <div
                className="absolute m-1"
                ref={grabElementRef}
                onMouseDown={() => onMouseDown()}
                onMouseMove={(e) => onMouseMove(e)}
                onMouseUp={onMouseUp}
                style={{
                    position: "absolute",
                    left: `${grabElementPos.x}px`,
                    top: `${grabElementPos.y}px`,
                    cursor: dragging ? "grabbing" : "grab",
                }}
            >
                {children}
            </div>
        </>
    )
}
