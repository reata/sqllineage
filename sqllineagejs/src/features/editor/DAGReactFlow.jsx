import React, { useCallback, useEffect, useMemo, useRef } from "react";
import {
  Background,
  Controls,
  ReactFlow,
  useNodesState,
  useEdgesState,
  getNodesBounds,
  getViewportForBounds,
  ControlButton,
} from "@xyflow/react";
import dagre from "dagre";
import { useDispatch, useSelector } from "react-redux";
import { ToggleButton, ToggleButtonGroup, Tooltip } from "@mui/material";
import TableChartIcon from "@mui/icons-material/TableChart";
import ViewWeekIcon from "@mui/icons-material/ViewWeek";
import SaveAltIcon from "@mui/icons-material/SaveAlt";
import AutorenewIcon from "@mui/icons-material/Autorenew";
import "@xyflow/react/dist/style.css";
import { toPng } from "html-to-image";

import { Loading } from "../widget/Loading.jsx";
import { LoadError } from "../widget/LoadError.jsx";

import { selectEditor, setDagLevel } from "./editorSlice.js";

function layoutWithDagre(nodes, edges, direction = "LR") {
  const dagreGraph = new dagre.graphlib.Graph();
  dagreGraph.setDefaultEdgeLabel(() => ({}));
  dagreGraph.setGraph({ rankdir: direction });

  const NODE_W = 180;
  const NODE_H = 36;

  nodes.forEach((n) =>
    dagreGraph.setNode(n.id, { width: NODE_W, height: NODE_H }),
  );
  edges.forEach((e) => dagreGraph.setEdge(e.source, e.target));

  dagre.layout(dagreGraph);

  return {
    nodes: nodes.map((n) => {
      const p = dagreGraph.node(n.id);
      return {
        ...n,
        position: {
          x: p.x - NODE_W / 2,
          y: p.y - NODE_H / 2,
        },
        positionAbsolute: undefined, // imply reactflow that position is absolute to avoid initial view shake
      };
    }),
    edges: edges,
  };
}

function formatReactFlow(nodes, edges) {
  const formatted_nodes = [];
  const formatted_edges = [];

  nodes.forEach((el) => {
    let style = el.type === "group" ? {style: {width: 170, height: 140}} : {};
    formatted_nodes.push({
      ...el,
      ...style,
      sourcePosition: "right",
      targetPosition: "left",
    });
  });

  edges.forEach((el) => {
    formatted_edges.push({
      ...el,
      animated: false,
      style: { stroke: "#9ab5c7" },
      markerEnd: { type: "arrowclosed", color: "#9ab5c7" },
    });
  });
  return layoutWithDagre(formatted_nodes, formatted_edges, "LR");
}

export function DAGReactFlow(props) {
  const dispatch = useDispatch();
  const editorState = useSelector(selectEditor);

  // redux state only stores the raw nodes and edges, reactFlowData includes position info
  const reactFlowData = useMemo(() => {
    if (editorState.dagLevel === "table") {
      return formatReactFlow(
        editorState.tableLineage.nodes,
        editorState.tableLineage.edges,
      );
    } else {
      return formatReactFlow(
        editorState.columnLineage.nodes,
        editorState.columnLineage.edges,
      );
    }
  }, [
    editorState.dagLevel,
    editorState.tableLineage,
    editorState.columnLineage,
  ]);

  const reactFlowWrapper = useRef(null);
  const reactFlowInstance = useRef(null);
  // use reactflow hooks to manage nodes and edges state
  const [nodes, setNodes, onNodesChange] = useNodesState(reactFlowData.nodes);
  const [edges, setEdges] = useEdgesState(reactFlowData.edges);

  // when cyto changes, run layout once, and then node dragging will only update local state
  useEffect(() => {
    setNodes(reactFlowData.nodes);
    setEdges(reactFlowData.edges);
    queueMicrotask(() => {
      reactFlowInstance.current?.fitView({ padding: 0.2 });
    });
  }, [reactFlowData, setNodes, setEdges]);

  const handleRelayout = useCallback(() => {
    setNodes((prev) => layoutWithDagre(prev, edges, "LR").nodes);
    queueMicrotask(() => {
      reactFlowInstance.current?.fitView({ padding: 0.2 });
    });
  }, [edges, setNodes]);

  const handleSave = useCallback(() => {
    let imageWidth = 4096;
    let imageHeight = 2160;
    const nodesBounds = getNodesBounds(reactFlowInstance.current?.getNodes());
    const viewPoint = getViewportForBounds(
      nodesBounds,
      imageWidth,
      imageHeight,
      0.5,
      2,
      0.2,
    );
    toPng(document.querySelector(".react-flow__viewport"), {
      backgroundColor: "white",
      width: imageWidth,
      height: imageHeight,
      style: {
        width: imageWidth,
        height: imageHeight,
        transform: `translate(${viewPoint.x}px, ${viewPoint.y}px) scale(${viewPoint.zoom})`,
      },
    }).then((dataUrl) => {
      const aDownloadLink = document.createElement("a");
      aDownloadLink.download = `${editorState.file}.png`;
      aDownloadLink.href = dataUrl;
      aDownloadLink.click();
    });
  }, [editorState.file]);

  const handleLevel = (_e, value) => {
    dispatch(setDagLevel(value));
  };

  if (editorState.dagStatus === "loading") {
    return <Loading minHeight={props.height} />;
  } else if (editorState.dagStatus === "failed") {
    return (
      <LoadError
        minHeight={props.height}
        message={
          editorState.dagError +
          "\nPlease check your SQL code for potential syntax error in Script View."
        }
      />
    );
  } else if (editorState.dagContent.length === 0) {
    let message,
      info = false;
    if (editorState.editable) {
      if (editorState.contentComposed === "") {
        message =
          "Welcome to SQLLineage Playground.\n" +
          "Just paste your SQL in Script View and switch back here, you'll get DAG visualization for your SQL code.\n" +
          "Or select SQL file on the left directory tree for visualization.\n" +
          "Have fun!";
        info = true;
      } else {
        message =
          "No Lineage Info found in your SQL.\nPlease review your code in Script View.";
      }
    } else {
      message = `No Lineage Info found in SQL file ${editorState.file}.\nPlease review the code in Script View.`;
    }
    return <LoadError minHeight={props.height} message={message} info={info} />;
  } else {
    return (
      <div
        style={{
          width: props.width,
          height: props.height,
          position: "relative",
        }}
        ref={reactFlowWrapper}
      >
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onInit={(instance) => (reactFlowInstance.current = instance)}
          onNodesChange={onNodesChange} // dragging nodes and update state
          nodesDraggable
          fitView
          minZoom={0.5}
          maxZoom={2}
          translateExtent={[
            [-1000, -1000],
            [3000, 3000],
          ]} // limit drag scope
        >
          <Controls>
            <ControlButton title={"Re-layout DAG"} onClick={handleRelayout}>
              <AutorenewIcon />
            </ControlButton>
            <ControlButton title={"Download DAG as PNG"} onClick={handleSave}>
              <SaveAltIcon />
            </ControlButton>
          </Controls>
          <Background />
        </ReactFlow>

        <ToggleButtonGroup
          orientation="vertical"
          value={editorState.dagLevel}
          exclusive
          onChange={handleLevel}
          aria-label="lineage level"
          sx={(theme) => ({
            position: "absolute",
            right: theme.spacing(0),
            bottom: theme.spacing(50),
            zIndex: 100,
          })}
        >
          <ToggleButton value="table">
            <Tooltip title="Table Level Lineage">
              <TableChartIcon />
            </Tooltip>
          </ToggleButton>
          <ToggleButton value="column">
            <Tooltip title="Column Level Lineage">
              <ViewWeekIcon />
            </Tooltip>
          </ToggleButton>
        </ToggleButtonGroup>
      </div>
    );
  }
}
