import React, {useRef} from 'react';
import CytoscapeComponent from 'react-cytoscapejs';
import dagre from 'cytoscape-dagre';
import cytoscape from 'cytoscape';
import {useSelector} from "react-redux";
import {selectEditor} from "./editorSlice";
import {Loading} from "../widget/Loading";
import {LoadError} from "../widget/LoadError";
import {SpeedDial, SpeedDialIcon} from "@material-ui/lab";
import {makeStyles} from "@material-ui/core/styles";
import SpeedDialAction from '@material-ui/lab/SpeedDialAction';
import SaveAltIcon from '@material-ui/icons/SaveAlt';
import ZoomInIcon from '@material-ui/icons/ZoomIn';
import SettingsBackupRestoreIcon from '@material-ui/icons/SettingsBackupRestore';
import ZoomOutIcon from '@material-ui/icons/ZoomOut';

cytoscape.use(dagre);

const useStyles = makeStyles((theme) => ({
  speedDial: {
    position: 'absolute',
    '&.MuiSpeedDial-directionLeft': {
      bottom: theme.spacing(12),
      right: theme.spacing(2),
    }
  },
}))


export function DAG(props) {
  const classes = useStyles();
  const editorState = useSelector(selectEditor);
  const [open, setOpen] = React.useState(false);
  const cyRef = useRef(null);

  const handleSave = () => {
    if (cyRef.current) {
      let cy = cyRef.current._cy;
      let aDownloadLink = document.createElement('a');
      aDownloadLink.download = `${editorState.file}.jpg`;
      aDownloadLink.href = cy.jpg({'full': true, 'quality': 1});
      aDownloadLink.click();
      setOpen(false);
    }
  }

  if (editorState.dagStatus === "loading") {
    return <Loading minHeight={props.height}/>
  } else if (editorState.dagStatus === "failed") {
    return <LoadError minHeight={props.height} message={editorState.dagError}/>
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
    return (
      <div>
        <CytoscapeComponent
          elements={editorState.dagContent}
          stylesheet={stylesheet}
          style={style}
          layout={layout}
          zoom={1}
          minZoom={0.5}
          maxZoom={2}
          wheelSensitivity={0.2}
          ref={cyRef}
        />
        <SpeedDial
          ariaLabel="SpeedDial example"
          className={classes.speedDial}
          hidden={false}
          icon={<SpeedDialIcon/>}
          onClose={() => setOpen(false)}
          onOpen={() => setOpen(true)}
          open={open}
          direction="left"
          FabProps={{"size": "small"}}
        >
          <SpeedDialAction
            title="Save"
            icon={<SaveAltIcon/>}
            onClick={handleSave}
          />
          <SpeedDialAction
            title="Zoom In"
            icon={<ZoomInIcon/>}
            onClick={() => {
              cyRef.current._cy.zoom(cyRef.current._cy.zoom() + 0.1)
            }}
          />
          <SpeedDialAction
            title="Auto Fit"
            icon={<SettingsBackupRestoreIcon/>}
            onClick={() => {
              cyRef.current._cy.fit()
            }}
          />
          <SpeedDialAction
            title="Zoom Out"
            icon={<ZoomOutIcon/>}
            onClick={() => {
              cyRef.current._cy.zoom(cyRef.current._cy.zoom() - 0.1)
            }}
          />
        </SpeedDial>
      </div>
    );
  }
}
