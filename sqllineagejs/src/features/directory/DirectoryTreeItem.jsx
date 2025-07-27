import React, { useEffect } from "react";
import { Box, Typography } from "@mui/material";
import { SimpleTreeView, TreeItem } from "@mui/x-tree-view";
import FolderIcon from "@mui/icons-material/Folder";
import DescriptionIcon from "@mui/icons-material/Description";
import { useDispatch, useSelector } from "react-redux";
import { useNavigate } from "react-router-dom";

import {
  DirectoryAPI,
  selectDirectory,
  setOpenNonSQLWarning,
} from "./directorySlice";

export default function DirectoryTreeItem(props) {
  const navigate = useNavigate();
  const dispatch = useDispatch();
  const directoryState = useSelector(selectDirectory);
  const [childNodes, setChildNodes] = React.useState(null);
  const [expanded, setExpanded] = React.useState([]);

  useEffect(() => {
    if (props.is_root) {
      setChildNodes(
        (directoryState.content.children ?? []).map((node) => (
          <DirectoryTreeItem
            id={node.id}
            name={node.name}
            is_dir={node.is_dir}
            is_root={false}
          />
        )),
      );
      setExpanded([props.id]);
    }
  }, [directoryState.content.children, props.id, props.is_root]);

  const handleSelectionChange = () => {
    if (!props.is_dir) {
      if (props.name.endsWith(".sql")) {
        navigate(`/?f=${props.id}`);
      } else {
        dispatch(setOpenNonSQLWarning(true));
      }
    }
  };

  const handleExpansionChange = (event, nodes) => {
    const expandingNodes = nodes.filter((x) => !expanded.includes(x));
    setExpanded(nodes);
    if (expandingNodes[0]) {
      const childId = expandingNodes[0];
      DirectoryAPI({ d: childId }).then((result) =>
        setChildNodes(
          result.children.map((node) => (
            <DirectoryTreeItem
              id={node.id}
              name={node.name}
              is_dir={node.is_dir}
              is_root={false}
            />
          )),
        ),
      );
    }
  };

  return (
    <SimpleTreeView
      sx={(theme) => ({ marginLeft: theme.spacing(0.2) })}
      expanded={expanded}
      onSelectedItemsChange={handleSelectionChange}
      onExpandedItemsChange={handleExpansionChange}
    >
      <TreeItem
        itemId={props.id}
        sx={{
          ".MuiTreeItem-content[data-selected]": {
            backgroundColor: "transparent",
          },
        }}
        label={
          <Box
            component="div"
            sx={(theme) => ({
              display: "flex",
              alignItems: "center",
              padding: theme.spacing(0.1, 0),
            })}
          >
            {props.is_dir ? (
              <FolderIcon
                color="action"
                sx={(theme) => ({ marginRight: theme.spacing(0.2) })}
              />
            ) : (
              <DescriptionIcon
                color="action"
                sx={(theme) => ({ marginRight: theme.spacing(0.2) })}
              />
            )}
            <Typography
              variant="body2"
              sx={{ fontWeight: "inherit", flexGrow: 1 }}
            >
              {props.name}
            </Typography>
          </Box>
        }
      >
        {props.is_dir && (childNodes || [<Box />])}
      </TreeItem>
    </SimpleTreeView>
  );
}
