import {TreeView} from "@material-ui/lab";
import ExpandMoreIcon from "@material-ui/icons/ExpandMore";
import ChevronRightIcon from "@material-ui/icons/ChevronRight";
import React, {useEffect} from "react";
import TreeItem from "@material-ui/lab/TreeItem";
import {makeStyles} from "@material-ui/core/styles";
import {useDispatch, useSelector} from "react-redux";
import {selectFileNodes, fetchDirectory, selectDirectory} from "./directorySlice";
import {Loading} from "../widget/Loading";
import {LoadError} from "../widget/LoadError";
import Typography from "@material-ui/core/Typography";
import FolderIcon from '@material-ui/icons/Folder';
import DescriptionIcon from '@material-ui/icons/Description';
import {useHistory} from "react-router-dom";
import {Snackbar} from "@material-ui/core";


const useStyles = makeStyles((theme) => ({
  directory: {
    marginLeft: theme.spacing(0.2)
  },
  labelRoot: {
    display: 'flex',
    alignItems: 'center',
    padding: theme.spacing(0.1, 0),
  },
  labelIcon: {
    marginRight: theme.spacing(0.2),
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
  const [open, setOpen] = React.useState(false);
  const fileNodes = useSelector(selectFileNodes);

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

  const handleSelect = (event, nodeId) => {
    if (fileNodes.has(nodeId)) {
      if (nodeId.endsWith(".sql")) {
        history.push(`/?f=${nodeId}`);
      } else {
        setOpen(true);
      }
    }
  };

  if (directoryState.status === "loading") {
    return <Loading minHeight={props.height}/>
  } else if (directoryState.status === "failed") {
    return <LoadError minHeight={props.height} message={directoryState.error}/>
  } else {
    return <div>
      <TreeView
        className={classes.directory}
        defaultCollapseIcon={<ExpandMoreIcon/>}
        defaultExpanded={['root']}
        defaultExpandIcon={<ChevronRightIcon/>}
        onNodeSelect={handleSelect}
      >
        {renderTree(directoryState.content)}
      </TreeView>
      <Snackbar
        anchorOrigin={{
          vertical: 'bottom',
          horizontal: 'left',
        }}
        open={open}
        autoHideDuration={1000}
        onClose={() => {
          setOpen(false)
        }}
        message="Non SQL File Is Not Supported"
      />
    </div>
  }
}
