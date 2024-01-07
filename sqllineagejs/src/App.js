import React, {useEffect, useMemo} from 'react';
import {
  AppBar,
  Box,
  Button,
  Drawer,
  Fade,
  FormControl,
  FormControlLabel,
  Grid,
  ListSubheader,
  Menu,
  MenuItem,
  Paper,
  Radio,
  RadioGroup,
  Toolbar,
  Tooltip,
  Typography
} from "@material-ui/core";
import {DAG} from "./features/editor/DAG";
import {Editor} from "./features/editor/Editor";
import {makeStyles} from "@material-ui/core/styles";
import ChevronLeftIcon from '@material-ui/icons/ChevronLeft';
import CreateIcon from "@material-ui/icons/Create";
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';
import IconButton from "@material-ui/core/IconButton";
import LanguageIcon from '@material-ui/icons/Language';
import MenuIcon from "@material-ui/icons/Menu";
import clsx from "clsx";
import {Directory} from "./features/directory/Directory";
import {BrowserRouter as Router, Link} from "react-router-dom";
import {DAGDesc} from "./features/editor/DAGDesc";
import {useSelector} from "react-redux";
import {selectEditor} from "./features/editor/editorSlice";


const useStyles = makeStyles((theme) => ({
  appBar: {
    flexGrow: 1,
    zIndex: theme.zIndex.drawer + 1,
  },
  menuButton: {
    marginRight: theme.spacing(2),
  },
  title: {
    flexGrow: 1,
  },
  content: {
    padding: theme.spacing(0.5),
    marginTop: theme.spacing(6),
    float: "right"
  },
  hide: {
    display: "none"
  },
  drawerPaper: {
    width: ({drawerWidth}) => drawerWidth + "vw",
  },
  contentShift: {
    transition: theme.transitions.create('margin', {
      easing: theme.transitions.easing.easeOut,
      duration: theme.transitions.duration.enteringScreen,
    }),
  },
  dragger: {
    width: '5px',
    cursor: 'ew-resize',
    position: 'absolute',
    top: 0,
    bottom: 0,
    left: ({drawerWidth}) => drawerWidth + "vw",
    backgroundColor: "transparent",
    zIndex: 999
  },
  dialect: {
    margin: theme.spacing(0, 0.5, 0, 1),
    display: 'none',
    [theme.breakpoints.up('md')]: {
      display: 'block',
    },
  }
}));


let isResizing = null;

const dialects = {
  "sqlfluff": [
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
    "tsql"
  ],
  "sqlparse": [
    "non-validating"
  ]
}


export default function App() {
  const editorState = useSelector(selectEditor);
  const [viewSelected, setViewSelected] = React.useState('dag');
  const [drawerOpen, setDrawerOpen] = React.useState(true);
  const [drawerWidth, setDrawerWidth] = React.useState(18);
  const [dialectMenuAnchor, setDialectMenuAnchor] = React.useState(null);
  const [dialectSelected, setDialectSelected] = React.useState("ansi");
  const classes = useStyles({drawerWidth: drawerWidth});

  const height = "90vh";
  const width = useMemo(() => {
    let full_width = 100;
    return (drawerOpen ? full_width - drawerWidth : full_width) + "vw"
  }, [drawerOpen, drawerWidth])

  const handleMouseDown = e => {
    e.stopPropagation();
    e.preventDefault();
    document.addEventListener("mousemove", handleMouseMove);
    document.addEventListener("mouseup", handleMouseUp)
    isResizing = true;
  };

  const handleMouseMove = e => {
    if (!isResizing) {
      return;
    }
    let width = e.clientX * 100 / window.innerWidth;
    let minWidth = 10, maxWidth = 50;
    if (width > minWidth && width < maxWidth) {
      setDrawerWidth(width);
    }
  }

  const handleMouseUp = () => {
    if (!isResizing) {
      return;
    }
    isResizing = false;
    document.removeEventListener("mousemove", handleMouseMove);
    document.removeEventListener("mouseup", handleMouseUp);
  }

  const handleDialectMenuClose = (e) => {
    let dialect = e.currentTarget.outerText;
    if (dialect !== "") {
      localStorage.setItem("dialect", dialect);
      setDialectSelected(dialect);
    }
    setDialectMenuAnchor(null);
  }

  useEffect(() => {
    let dialect = localStorage.getItem("dialect");
    if (dialect !== null) {
      setDialectSelected(dialect);
    }
  }, [])

  return (
    <Router basename={process.env.PUBLIC_URL}>
      <div>
        <Box>
          <AppBar position="fixed" className={classes.appBar}>
            <Toolbar variant="dense">
              <IconButton
                edge="start"
                className={classes.menuButton}
                color="inherit"
                aria-label="menu"
                onClick={() => {
                  setDrawerOpen(!drawerOpen)
                }}
              >
                {drawerOpen ? <ChevronLeftIcon/> : <MenuIcon/>}
              </IconButton>
              <Typography variant="h6" className={classes.title}>
                SQLLineage
              </Typography>

              <Tooltip title="Change SQL dialect" arrow>
                <Button
                  color="inherit"
                  onClick={(event) => {
                    setDialectMenuAnchor(event.currentTarget)
                  }}
                >
                  <LanguageIcon/>
                  <span className={classes.dialect}>{dialectSelected}</span>
                  <ExpandMoreIcon fontSize="small"/>
                </Button>
              </Tooltip>
              <Menu
                id="dialect-menu"
                anchorEl={dialectMenuAnchor}
                open={Boolean(dialectMenuAnchor)}
                onClose={handleDialectMenuClose}
                TransitionComponent={Fade}
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
                      )
                    )}
                  </div>
                ))}
              </Menu>

              {editorState.editable ?
                <Tooltip title="Visualize Lineage By Filling In Your Own SQL" arrow>
                  <div>Composing Mode</div>
                </Tooltip> :
                <Link to="/" style={{color: "white"}}>
                  <Tooltip title="Enter Composing Mode to Visualize Your Own SQL" arrow>
                    <IconButton
                      color="inherit"
                      onClick={() => {
                        setViewSelected("script");
                        setDrawerOpen(false);
                      }}
                    >
                      <CreateIcon/>
                    </IconButton>
                  </Tooltip>
                </Link>
              }
            </Toolbar>
          </AppBar>
          <Drawer
            variant="persistent"
            open={drawerOpen}
            classes={{
              paper: classes.drawerPaper,
            }}
          >
            <Box className={classes.content}>
              <Directory/>
            </Box>
          </Drawer>
        </Box>
        <div
          id="dragger"
          onMouseDown={handleMouseDown}
          className={clsx(classes.dragger, {[classes.hide]: !drawerOpen})}
        />
        <main
          className={clsx(classes.content, {[classes.contentShift]: drawerOpen})}
        >
          <Paper elevation="24" style={{height: height, width: width}}>
            <Box className={viewSelected === "dag" ? "" : classes.hide}>
              <DAG height={height} width={width}/>
            </Box>
            <Box className={viewSelected === "text" ? "" : classes.hide}>
              <DAGDesc height={height} width={width}/>
            </Box>
            <Box className={viewSelected === "script" ? "" : classes.hide}>
              <Editor height={height} width={width} dialect={dialectSelected}/>
            </Box>
          </Paper>
          <Grid container justify="center">
            <FormControl component="fieldset">
              <RadioGroup row aria-label="position" name="position" defaultValue="dag"
                          value={viewSelected}
                          onChange={(event) => setViewSelected(event.target.value)}>
                <FormControlLabel
                  value="dag"
                  control={<Radio color="primary"/>}
                  label="Lineage View"
                />
                <FormControlLabel
                  value="text"
                  control={<Radio color="primary"/>}
                  label="Text View"
                />
                <FormControlLabel
                  value="script"
                  control={<Radio color="primary"/>}
                  label="Script View"
                />
              </RadioGroup>
            </FormControl>
          </Grid>
        </main>
      </div>
    </Router>
  )
}
