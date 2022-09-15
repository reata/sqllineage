import React, {useMemo} from 'react';
import {Box, Drawer, FormControl, FormControlLabel, Grid, Paper, Radio, RadioGroup, Tooltip} from "@material-ui/core";
import {DAG} from "./features/editor/DAG";
import {Editor} from "./features/editor/Editor";
import {makeStyles} from "@material-ui/core/styles";
import AppBar from "@material-ui/core/AppBar";
import Toolbar from "@material-ui/core/Toolbar";
import IconButton from "@material-ui/core/IconButton";
import MenuIcon from "@material-ui/icons/Menu";
import CreateIcon from "@material-ui/icons/Create";
import Typography from "@material-ui/core/Typography";
import ChevronLeftIcon from '@material-ui/icons/ChevronLeft';
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
  }
}));


let isResizing = null;


export default function App() {
  const editorState = useSelector(selectEditor);
  const [selectedValue, setSelectedValue] = React.useState('dag');
  const [open, setOpen] = React.useState(true);
  const [drawerWidth, setDrawerWidth] = React.useState(18);
  const classes = useStyles({drawerWidth: drawerWidth});

  const height = "90vh";
  const width = useMemo(() => {
    let full_width = 100;
    return (open ? full_width - drawerWidth : full_width) + "vw"
  }, [open, drawerWidth])

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
                  setOpen(!open)
                }}
              >
                {open ? <ChevronLeftIcon/> : <MenuIcon/>}
              </IconButton>
              <Typography variant="h6" className={classes.title}>
                SQLLineage
              </Typography>
              {editorState.editable ?
                <Tooltip title="Visualize Lineage By Filling In Your Own SQL" arrow>
                  <div>Composing Mode</div>
                </Tooltip> :
                <Link to="/" style={{color: "white"}}>
                  <Tooltip title="Enter Composing Mode to Visualize Your Own SQL" arrow>
                    <IconButton
                      color="inherit"
                      onClick={() => {
                        setSelectedValue("script");
                        setOpen(false);
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
            open={open}
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
          className={clsx(classes.dragger, {[classes.hide]: !open})}
        />
        <main
          className={clsx(classes.content, {[classes.contentShift]: open})}
        >
          <Paper elevation="24" style={{height: height, width: width}}>
            <Box className={selectedValue === "dag" ? "" : classes.hide}>
              <DAG height={height} width={width}/>
            </Box>
            <Box className={selectedValue === "text" ? "" : classes.hide}>
              <DAGDesc height={height} width={width}/>
            </Box>
            <Box className={selectedValue === "script" ? "" : classes.hide}>
              <Editor height={height} width={width}/>
            </Box>
          </Paper>
          <Grid container justify="center">
            <FormControl component="fieldset">
              <RadioGroup row aria-label="position" name="position" defaultValue="dag"
                          value={selectedValue}
                          onChange={(event) => setSelectedValue(event.target.value)}>
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
