import React, {useEffect} from "react";
import Typography from "@material-ui/core/Typography";
import TreeView from "@material-ui/lab/TreeView";
import TreeItem from "@material-ui/lab/TreeItem";
import ExpandMoreIcon from "@material-ui/icons/ExpandMore";
import ChevronRightIcon from "@material-ui/icons/ChevronRight";
import FolderIcon from "@material-ui/icons/Folder";
import DescriptionIcon from "@material-ui/icons/Description";
import {makeStyles} from "@material-ui/core";
import {useDispatch, useSelector} from "react-redux";
import {DirectoryAPI, selectDirectory, setOpenNonSQLWarning} from "./directorySlice";
import {useHistory} from "react-router-dom";


const useStyles = makeStyles((theme) => ({
  "@global": {
    ".MuiTreeItem-root.Mui-selected > .MuiTreeItem-content .MuiTreeItem-label": {
      backgroundColor: "white"
    },
  },
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


export default function DirectoryTreeItem(props) {
  const classes = useStyles();
  const history = useHistory();
  const dispatch = useDispatch();
  const directoryState = useSelector(selectDirectory);
  const [childNodes, setChildNodes] = React.useState(null);
  const [expanded, setExpanded] = React.useState([]);

  useEffect(() => {
    if (props.is_root) {
      setChildNodes((directoryState.content.children ?? []).map(
        node => <DirectoryTreeItem id={node.id} name={node.name} is_dir={node.is_dir} is_root={false}/>)
      )
      setExpanded([props.id])
    }
  }, [directoryState.content.children, props.id, props.is_root])

  const handleSelect = () => {
    if (!props.is_dir) {
      if (props.name.endsWith(".sql")) {
        history.push(`/?f=${props.id}`);
      } else {
        dispatch(setOpenNonSQLWarning(true))
      }
    }
  };

  const handleToggle = (event, nodes) => {
    const expandingNodes = nodes.filter(x => !expanded.includes(x));
    setExpanded(nodes);
    if (expandingNodes[0]) {
      const childId = expandingNodes[0];
      DirectoryAPI({"d": childId}).then(result =>
        setChildNodes(
          result.children.map(node => <DirectoryTreeItem id={node.id} name={node.name} is_dir={node.is_dir}
                                                         is_root={false}/>)
        )
      );
    }
  };

  return (
    <TreeView
      className={classes.directory}
      defaultCollapseIcon={<ExpandMoreIcon/>}
      defaultExpandIcon={<ChevronRightIcon/>}
      expanded={expanded}
      onNodeSelect={handleSelect}
      onNodeToggle={handleToggle}
    >
      <TreeItem
        key={props.id}
        nodeId={props.id}
        label={
          <div className={classes.labelRoot}>
            {props.is_dir ? <FolderIcon color="action" className={classes.labelIcon}/> :
              <DescriptionIcon color="action" className={classes.labelIcon}/>}
            <Typography variant="body2" className={classes.labelText}>
              {props.name}
            </Typography>
          </div>
        }
      >
        {props.is_dir && (childNodes || [<div key="stub"/>])}
      </TreeItem>
    </TreeView>
  );
}
