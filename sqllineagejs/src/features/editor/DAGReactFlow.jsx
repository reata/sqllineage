import React, {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import {
  Background,
  Controls,
  MiniMap,
  ReactFlow,
  useNodesState,
  useEdgesState,
} from "@xyflow/react";
import dagre from "dagre";
import { useDispatch, useSelector } from "react-redux";
import { selectEditor, setDagLevel } from "./editorSlice.js";
import { Loading } from "../widget/Loading.jsx";
import { LoadError } from "../widget/LoadError.jsx";
import {
  SpeedDial,
  SpeedDialIcon,
  ToggleButton,
  ToggleButtonGroup,
  Tooltip,
} from "@mui/material";
import SpeedDialAction from "@mui/material/SpeedDialAction";
import TableChartIcon from "@mui/icons-material/TableChart";
import ViewWeekIcon from "@mui/icons-material/ViewWeek";
import SaveAltIcon from "@mui/icons-material/SaveAlt";
import AutorenewIcon from "@mui/icons-material/Autorenew";
import "@xyflow/react/dist/style.css";

// convert cytoscape elements to react flow nodes and edges
function cytoToReactFlow(elements) {
  const nodes = [];
  const edges = [];

  elements.forEach((el) => {
    if (el.data && el.data.id) {
      if (el.group === "nodes" || el.data.source === undefined) {
        nodes.push({
          id: el.data.id,
          data: { label: el.data.id, ...el.data },
          position: {
            // placeholder, true position will be set by dagre
            x: el.position?.x ?? Math.random() * 400,
            y: el.position?.y ?? Math.random() * 400,
          },
          type: "default",
          sourcePosition: "right",
          targetPosition: "left",
        });
      } else if (el.group === "edges" || el.data.source) {
        edges.push({
          id: el.data.id || `${el.data.source}-${el.data.target}`,
          source: el.data.source,
          target: el.data.target,
          animated: false,
          style: { stroke: "#9ab5c7" },
          markerEnd: { type: "arrowclosed", color: "#9ab5c7" },
        });
      }
    }
  });
  return { nodes, edges };
}

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

  return nodes.map((n) => {
    const p = dagreGraph.node(n.id);
    return {
      ...n,
      position: {
        x: p.x - NODE_W / 2,
        y: p.y - NODE_H / 2,
      },
      positionAbsolute: undefined, // imply reactflow that position is absolute to avoid initial view shake
    };
  });
}

export function DAGReactFlow(props) {
  const dispatch = useDispatch();
  const editorState = useSelector(selectEditor);

  const [open, setOpen] = useState(false);
  const reactFlowWrapper = useRef(null);
  const reactFlowInstance = useRef(null);

  // only update reactflow nodes/edges when dag changes
  const cyto = useMemo(
    () => cytoToReactFlow(editorState.dagContent),
    [editorState.dagContent],
  );

  // use reactflow hooks to manage nodes and edges state
  const [nodes, setNodes, onNodesChange] = useNodesState(cyto.nodes);
  const [edges, setEdges] = useEdgesState(cyto.edges);

  // when cyto changes, run layout once, and then node dragging will only update local state
  useEffect(() => {
    const layouted = layoutWithDagre(cyto.nodes, cyto.edges, "LR");
    setNodes(layouted);
    setEdges(cyto.edges);
    queueMicrotask(() => {
      reactFlowInstance.current?.fitView({ padding: 0.2 });
    });
  }, [cyto.nodes, cyto.edges, setNodes, setEdges]);

  const handleRelayout = useCallback(() => {
    setNodes((prev) => layoutWithDagre(prev, edges, "LR"));
    queueMicrotask(() => {
      reactFlowInstance.current?.fitView({ padding: 0.2 });
    });
    setOpen(false);
  }, [edges, setNodes]);

  const handleSave = useCallback(() => {
    if (!reactFlowInstance.current) return;
    reactFlowInstance.current.toPng().then((dataUrl) => {
      const a = document.createElement("a");
      a.href = dataUrl;
      a.download = `${editorState.file}.png`;
      setOpen(false);
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
          <MiniMap />
          <Controls />
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
        <SpeedDial
          ariaLabel="SpeedDial example"
          sx={(theme) => ({
            position: "absolute",
            bottom: theme.spacing(5),
            right: theme.spacing(2),
          })}
          hidden={false}
          icon={<SpeedDialIcon />}
          onClose={() => setOpen(false)}
          onOpen={() => setOpen(true)}
          open={open}
          direction="left"
          FabProps={{ size: "small" }}
        >
          <SpeedDialAction
            title="Save"
            icon={<SaveAltIcon />}
            onClick={handleSave}
          />
          <SpeedDialAction
            title="Relayout"
            icon={<AutorenewIcon />}
            onClick={handleRelayout}
          />
        </SpeedDial>
      </div>
    );
  }
}
