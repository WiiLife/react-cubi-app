import { useRef, useState, useCallback } from "react";
import type { MouseEvent, ReactNode, RefObject } from "react";

type GrapComponentProps = {
    containerRef: RefObject<HTMLDivElement | null>;
    colContainerRef: RefObject<HTMLDivElement | null>;
    pivContainerRef: RefObject<HTMLDivElement | null>;
    children: ReactNode;
};

export default function GrapComponent({ containerRef, colContainerRef, pivContainerRef, children }: GrapComponentProps) {
    const grabComponentRef = useRef<HTMLDivElement>(null);
    const [grabComponentPos, setGrabComponentPos] = useState<{x: number, y: number}>({x: 0, y: 0});
    const dragState = useRef({ x: 0, y: 0 });
    const [dragging, setDragging] = useState<boolean>(false);

    const onMouseDown = useCallback((e: MouseEvent<HTMLDivElement>) => {
        if (!grabComponentRef.current || !containerRef.current) return

        const sectRect = containerRef.current.getBoundingClientRect()
        setDragging(true);
        dragState.current = {
            x: e.clientX - sectRect.left - grabComponentPos.x,
            y: e.clientY - sectRect.top - grabComponentPos.y
        }

    }, [grabComponentPos, containerRef])

    const onMouseMove = useCallback((e: MouseEvent<HTMLDivElement>) => {
        if (!dragging || !containerRef.current) return

        const sectRect = containerRef.current.getBoundingClientRect()
        setGrabComponentPos({
            x: e.clientX - sectRect.left - dragState.current.x,
            y: e.clientY - sectRect.top - dragState.current.y
        })
    }, [dragging, containerRef])

    const onMouseUp = () => {
        setDragging(false)
    }

    function snapToSection() {
    
        // snap to section has to happen onMouseUp
        // currently the objects are on top of one other, need to modify onMouseDown to prevent that
        // current snapToSection snaps object one on top of another, give it some clearance by the number of elements in the list

        if (!grabComponentRef.current || !containerRef.current || !colContainerRef.current || !pivContainerRef.current) {
            return undefined;
        }

        const objRect = grabComponentRef.current.getBoundingClientRect();
        const colSecRect = colContainerRef.current.getBoundingClientRect();
        const pivSecRect = pivContainerRef.current.getBoundingClientRect();

        const distToColSection = (colSecRect.x - objRect.x) ** 2 + (colSecRect.y - objRect.y) ** 2;
        const distToPivSection = (pivSecRect.x - objRect.x) ** 2 + (pivSecRect.y - objRect.y) ** 2;

        const closestRect = distToColSection < distToPivSection ? colSecRect : pivSecRect;
        
        return {
            x: closestRect.x,
            y: closestRect.y
        };
    }

    return (
        <>
            <div
                className="absolute"
                ref={grabComponentRef}
                onMouseDown={(e) => onMouseDown(e)}
                onMouseMove={(e) => onMouseMove(e)}
                onMouseUp={onMouseUp}
                style={{
                    position: "absolute",
                    left: `${grabComponentPos.x}px`,
                    top: `${grabComponentPos.y}px`,
                    cursor: dragging ? "grabbing" : "grab",
                }}
            >
                {children}
            </div>
        </>
    )
}
