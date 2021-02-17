import React, {useEffect} from 'react';
import CytoscapeComponent from 'react-cytoscapejs';
import dagre from 'cytoscape-dagre';
import cytoscape from 'cytoscape';
import {useDispatch, useSelector} from "react-redux";
import {fetchDAG, selectDAG, setFile} from "./dagSlice";
import {Loading} from "../widget/Loading";
import {LoadError} from "../widget/LoadError";
import {useLocation} from "react-router-dom";

cytoscape.use(dagre);

const useFile = () => {return (new URLSearchParams(useLocation().search)).get("f")};

export function DAG(props) {
  const dispatch = useDispatch();
  const dagState = useSelector(selectDAG);
  const file = useFile();

  useEffect(() => {
    if (dagState.file !== file) {
      dispatch(setFile(file));
      dispatch(fetchDAG());
    }
  })

  if (dagState.status === "loading") {
    return <Loading minHeight={props.height}/>
  } else if (dagState.status === "failed") {
    return <LoadError minHeight={props.height} message={dagState.error}/>
  } else {
    const stylesheet = [
      {
        selector: 'node',
        style: {
          height: 10,
          width: 10,
          content: 'data(id)',
          'text-valign': 'top',
          'text-halign': 'right',
          'font-size': 10,
          'color': '#35393e',
          'background-color': '#3499d9',
          'border-color': '#000',
          'border-width': 1,
          'border-opacity': 0.8
        }
      },
      {
        selector: 'edge',
        style: {
          width: 1,
          'line-color': '#9ab5c7',
          'target-arrow-color': '#9ab5c7',
          'target-arrow-shape': 'triangle',
          'arrow-scale': 0.8,
          'curve-style': 'bezier'
        }
      }
    ]
    const layout = {
      name: 'dagre',
      rankDir: 'LR',
      rankSep: 200,
    };
    const style = {width: props.width, height: props.height};
    return <CytoscapeComponent
      elements={dagState.content}
      stylesheet={stylesheet}
      style={style}
      layout={layout}
      zoom={1}
      minZoom={0.5}
      maxZoom={2}
      wheelSensitivity={0.2}
    />;
  }
}
