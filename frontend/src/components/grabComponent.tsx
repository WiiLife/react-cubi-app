import { useRef, useState, useCallback, useLayoutEffect} from "react";
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
    const draggingRef = useRef<{startX: number, startY: number}>({startX: 0, startY: 0});

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
                x: elemRect.x + elemRect.width / 2,
                y: elemRect.y + elemRect.height / 2
            })
        }
    }

    const resetGrabElementPos = useCallback(() => {
        if (!grabElementRef.current || !containerRef.current) return
        const elemRect = grabElementRef.current.getBoundingClientRect()
        const containerRect = containerRef.current.getBoundingClientRect()
        const elemSize = {w: elemRect.width, h: elemRect.height}

        setGrabElementPos({
            x: containerRect.left + 3,
            y: containerRect.top + (elemSize.h * index) + ((index + 1) * 3)
        })

        if (grabElementRef.current) {
            grabElementRef.current.style.transform = "none";
        }   

    }, [index, containerRef])

    const measureDistance = (startX: number, startY: number, endX: number, endY: number) => {
        return Math.sqrt((startX - endX) ** 2 + (startY - endY) ** 2)
    }

    useLayoutEffect(() => {
        resetGrabElementPos()
        window.addEventListener("resize", resetGrabElementPos)
        const resizeObserver = new ResizeObserver(resetGrabElementPos)
        resizeObserver.observe(document.documentElement)

        return () => {
            window.removeEventListener("resize", resetGrabElementPos)
            resizeObserver.disconnect()
        }
    }, [containerRef, otherRef, resetGrabElementPos])
    
    const onMouseDown = useCallback((e: MouseEvent<HTMLDivElement>) => {
        if (!grabElementRef.current || !draggingRef.current|| !containerRef.current) return
        e.preventDefault();
        e.stopPropagation();
        setDragging(true);
        draggingRef.current = {
            startX: e.clientX,
            startY: e.clientY
        }
    }, [containerRef])

    const onMouseUp = useCallback(() => {
        if (!grabElementRef.current || !containerRef.current) return
        setDragging(false);

        const containerCenter = getCenterOfElement(containerRef, false);
        const otherCenter = getCenterOfElement(otherRef, false);
        const elmCenter = getCenterOfElement(grabElementRef, false);

        const containerCenterDist = measureDistance(containerCenter.x, containerCenter.y, elmCenter.x, elmCenter.y);
        const otherCenterDist = measureDistance(otherCenter.x, otherCenter.y, elmCenter.x, elmCenter.y);

        if (containerCenterDist > otherCenterDist) {
            togglePivotColumn(col, "toggle");
        }
        resetGrabElementPos();
    }, [col, containerRef, otherRef, resetGrabElementPos, togglePivotColumn])

    const onMouseMove = useCallback((e: MouseEvent<HTMLDivElement>) => {
        if (!dragging || !draggingRef.current || !grabElementRef.current) return

        const deltaX = e.clientX - draggingRef.current.startX;
        const deltaY = e.clientY - draggingRef.current.startY;
        const distance = Math.sqrt(deltaX * deltaX + deltaY * deltaY);
        
        if (distance < 10) return

        grabElementRef.current.style.transform = `translate(${deltaX}px, ${deltaY}px)`;

    }, [dragging, draggingRef])

    return (
        <>
            <div
                className="absolute m-1"
                ref={grabElementRef}
                onMouseDown={(e) => onMouseDown(e)}
                onMouseMove={(e) => onMouseMove(e)}
                onMouseUp={onMouseUp}
                onMouseLeave={onMouseUp}
                style={{
                    position: "absolute",
                    left: `${grabElementPos.x}px`,
                    top: `${grabElementPos.y}px`,
                    cursor: dragging ? "grabbing" : "grab",
                    zIndex: dragging ? 9999 : undefined
                }}
            >
                {children}
            </div>
        </>
    )
}
