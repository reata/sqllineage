import {TreeView} from "@material-ui/lab";
import ExpandMoreIcon from "@material-ui/icons/ExpandMore";
import ChevronRightIcon from "@material-ui/icons/ChevronRight";
import React, {useEffect} from "react";
import TreeItem from "@material-ui/lab/TreeItem";
import {makeStyles} from "@material-ui/core/styles";
import {useDispatch, useSelector} from "react-redux";
import {fetchDirectory, selectDirectory} from "./directorySlice";
import {Loading} from "../widget/Loading";
import {LoadError} from "../widget/LoadError";
import Typography from "@material-ui/core/Typography";
import FolderIcon from '@material-ui/icons/Folder';
import DescriptionIcon from '@material-ui/icons/Description';
import { useHistory } from "react-router-dom";


const useStyles = makeStyles((theme) => ({
  directory: {
    margin: theme.spacing(1)
  },
  labelRoot: {
    display: 'flex',
    alignItems: 'center',
    padding: theme.spacing(0.5, 0),
  },
  labelIcon: {
    marginRight: theme.spacing(1),
  },
  labelText: {
    fontWeight: 'inherit',
    flexGrow: 1,
  },
}));

function StyledTreeItem(props) {
  const classes = useStyles();
  const {labelText, labelIcon: LabelIcon, ...other} = props;

  return (
    <TreeItem
      label={
        <div className={classes.labelRoot}>
          <LabelIcon color="action" className={classes.labelIcon}/>
          <Typography variant="body2" className={classes.labelText}>
            {labelText}
          </Typography>
        </div>
      }
      {...other}
    />
  );
}


export function Directory(props) {
  const classes = useStyles();
  const dispatch = useDispatch();
  const directoryState = useSelector(selectDirectory);
  const history = useHistory();

  useEffect(() => {
    if (directoryState.status === "idle") {
      dispatch(fetchDirectory())
    }
  })

  const renderTree = (nodes) => (
    <StyledTreeItem key={nodes.id} nodeId={nodes.id} labelText={nodes.name}
                    labelIcon={Array.isArray(nodes.children) ? FolderIcon : DescriptionIcon}>
      {Array.isArray(nodes.children) ? nodes.children.map((node) => renderTree(node)) : null}
    </StyledTreeItem>
  );

  const handleSelect = (event, nodeIds) => {
    if (nodeIds.endsWith(".sql")) {
      history.push(`/?f=${nodeIds}`);
    } else {
      console.log("skip non sql file");
    }
  };

  if (directoryState.status === "loading") {
    return <Loading minHeight={props.height}/>
  } else if (directoryState.status === "failed") {
    return <LoadError minHeight={props.height} message={directoryState.error}/>
  } else {
    return <TreeView
      className={classes.directory}
      defaultCollapseIcon={<ExpandMoreIcon/>}
      defaultExpanded={['root']}
      defaultExpandIcon={<ChevronRightIcon/>}
      onNodeSelect={handleSelect}
    >
      {renderTree(directoryState.content)}
    </TreeView>
  }
}
