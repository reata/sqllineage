import React, { useEffect, useMemo } from "react";
import {
  AppBar,
  Box,
  Button,
  Drawer,
  Fade,
  FormControl,
  FormControlLabel,
  Grid,
  IconButton,
  ListSubheader,
  Menu,
  MenuItem,
  Paper,
  Radio,
  RadioGroup,
  Toolbar,
  Tooltip,
  Typography,
} from "@mui/material";
import ChevronLeftIcon from "@mui/icons-material/ChevronLeft";
import CreateIcon from "@mui/icons-material/Create";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import LanguageIcon from "@mui/icons-material/Language";
import MenuIcon from "@mui/icons-material/Menu";
import { BrowserRouter, Link } from "react-router-dom";
import { useSelector } from "react-redux";

import { Directory } from "./features/directory/Directory";
import { DAGDesc } from "./features/editor/DAGDesc";
import { Editor } from "./features/editor/Editor";
import { DAG } from "./features/editor/DAG";
import { selectEditor } from "./features/editor/editorSlice";
import { BASE_URL } from "./config.js";

let isResizing = null;

const dialects = {
  sqlfluff: [
    "ansi",
    "athena",
    "bigquery",
    "clickhouse",
    "databricks",
    "db2",
    "duckdb",
    "exasol",
    "greenplum",
    "hive",
    "materialize",
    "mysql",
    "oracle",
    "postgres",
    "redshift",
    "snowflake",
    "soql",
    "sparksql",
    "sqlite",
    "teradata",
    "trino",
    "tsql",
  ],
  sqlparse: ["non-validating"],
};

export default function App() {
  const editorState = useSelector(selectEditor);
  const [viewSelected, setViewSelected] = React.useState("dag");
  const [drawerOpen, setDrawerOpen] = React.useState(true);
  const [drawerWidth, setDrawerWidth] = React.useState(18);
  const [dialectMenuAnchor, setDialectMenuAnchor] = React.useState(null);
  const [dialectSelected, setDialectSelected] = React.useState("ansi");

  const height = "90vh";
  const width = useMemo(() => {
    let full_width = 100;
    return (drawerOpen ? full_width - drawerWidth : full_width) + "vw";
  }, [drawerOpen, drawerWidth]);

  const handleMouseDown = (e) => {
    e.stopPropagation();
    e.preventDefault();
    document.addEventListener("mousemove", handleMouseMove);
    document.addEventListener("mouseup", handleMouseUp);
    isResizing = true;
  };

  const handleMouseMove = (e) => {
    if (!isResizing) {
      return;
    }
    let width = (e.clientX * 100) / window.innerWidth;
    let minWidth = 10,
      maxWidth = 50;
    if (width > minWidth && width < maxWidth) {
      setDrawerWidth(width);
    }
  };

  const handleMouseUp = () => {
    if (!isResizing) {
      return;
    }
    isResizing = false;
    document.removeEventListener("mousemove", handleMouseMove);
    document.removeEventListener("mouseup", handleMouseUp);
  };

  const handleDialectMenuClose = (e) => {
    let dialect = e.currentTarget.outerText;
    if (dialect !== "") {
      localStorage.setItem("dialect", dialect);
      setDialectSelected(dialect);
    }
    setDialectMenuAnchor(null);
  };

  useEffect(() => {
    let dialect = localStorage.getItem("dialect");
    if (dialect !== null) {
      setDialectSelected(dialect);
    }
  }, []);

  return (
    <BrowserRouter basename={BASE_URL}>
      <div>
        <Box>
          <AppBar
            position="fixed"
            sx={(theme) => ({
              flexGrow: 1,
              zIndex: theme.zIndex.drawer + 1,
            })}
          >
            <Toolbar variant="dense">
              <IconButton
                edge="start"
                sx={(theme) => ({ marginRight: theme.spacing(2) })}
                color="inherit"
                aria-label="menu"
                onClick={() => {
                  setDrawerOpen(!drawerOpen);
                }}
                size="large"
              >
                {drawerOpen ? <ChevronLeftIcon /> : <MenuIcon />}
              </IconButton>
              <Typography variant="h6" sx={{ flexGrow: 1 }}>
                SQLLineage
              </Typography>

              <Tooltip title="Change SQL dialect" arrow>
                <Button
                  color="inherit"
                  onClick={(event) => {
                    setDialectMenuAnchor(event.currentTarget);
                  }}
                >
                  <LanguageIcon />
                  <Box
                    component="span"
                    sx={(theme) => ({
                      margin: theme.spacing(0, 0.5, 0, 1),
                      display: { xs: "none", md: "block" },
                    })}
                  >
                    {dialectSelected}
                  </Box>
                  <ExpandMoreIcon fontSize="small" />
                </Button>
              </Tooltip>
              <Menu
                id="dialect-menu"
                anchorEl={dialectMenuAnchor}
                open={Boolean(dialectMenuAnchor)}
                onClose={handleDialectMenuClose}
                slots={{
                  transition: Fade,
                }}
              >
                {Object.entries(dialects).map((entry) => (
                  <div>
                    <ListSubheader>{entry[0]}</ListSubheader>
                    {entry[1].map((dialect) => (
                      <MenuItem
                        selected={dialect === dialectSelected}
                        value={dialect}
                        onClick={handleDialectMenuClose}
                      >
                        {dialect}
                      </MenuItem>
                    ))}
                  </div>
                ))}
              </Menu>

              {editorState.editable ? (
                <Tooltip
                  title="Visualize Lineage By Filling In Your Own SQL"
                  arrow
                >
                  <div>Composing Mode</div>
                </Tooltip>
              ) : (
                <Link to="/" style={{ color: "white" }}>
                  <Tooltip
                    title="Enter Composing Mode to Visualize Your Own SQL"
                    arrow
                  >
                    <IconButton
                      color="inherit"
                      onClick={() => {
                        setViewSelected("script");
                        setDrawerOpen(false);
                      }}
                      size="large"
                    >
                      <CreateIcon />
                    </IconButton>
                  </Tooltip>
                </Link>
              )}
            </Toolbar>
          </AppBar>
          <Drawer variant="persistent" open={drawerOpen}>
            <Box
              sx={(theme) => ({
                padding: theme.spacing(0.5),
                marginTop: theme.spacing(6),
                float: "right",
                width: drawerWidth + "vw",
              })}
            >
              <Directory />
            </Box>
          </Drawer>
        </Box>
        <Box
          id="dragger"
          onMouseDown={handleMouseDown}
          component="div"
          sx={{
            width: "5px",
            cursor: "ew-resize",
            position: "absolute",
            top: 0,
            bottom: 0,
            backgroundColor: "transparent",
            zIndex: 999,
            display: drawerOpen ? "block" : "none",
            left: drawerWidth + 0.6 + "vw",
          }}
        />
        <Box
          component="main"
          sx={(theme) => ({
            padding: theme.spacing(0.5),
            marginTop: theme.spacing(6),
            float: "right",
            marginLeft: drawerOpen ? drawerWidth + "vw" : theme.spacing(0),
          })}
        >
          <Paper elevation="24" style={{ height: height, width: width }}>
            <Box sx={viewSelected === "dag" ? {} : { display: "none" }}>
              <DAG height={height} width={width} />
            </Box>
            <Box sx={viewSelected === "text" ? {} : { display: "none" }}>
              <DAGDesc height={height} width={width} />
            </Box>
            <Box sx={viewSelected === "script" ? {} : { display: "none" }}>
              <Editor height={height} width={width} dialect={dialectSelected} />
            </Box>
          </Paper>
          <Grid container justifyContent="center">
            <FormControl variant="standard" component="fieldset">
              <RadioGroup
                row
                aria-label="position"
                name="position"
                defaultValue="dag"
                value={viewSelected}
                onChange={(event) => setViewSelected(event.target.value)}
              >
                <FormControlLabel
                  value="dag"
                  control={<Radio color="primary" />}
                  label="Lineage View"
                />
                <FormControlLabel
                  value="text"
                  control={<Radio color="primary" />}
                  label="Text View"
                />
                <FormControlLabel
                  value="script"
                  control={<Radio color="primary" />}
                  label="Script View"
                />
              </RadioGroup>
            </FormControl>
          </Grid>
        </Box>
      </div>
    </BrowserRouter>
  );
}
