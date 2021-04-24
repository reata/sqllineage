import React, {useEffect} from "react";
import {useDispatch, useSelector} from "react-redux";
import {fetchRootDirectory, selectDirectory, setOpenNonSQLWarning} from "./directorySlice";
import {Loading} from "../widget/Loading";
import {LoadError} from "../widget/LoadError";
import {Snackbar} from "@material-ui/core";
import DirectoryTreeItem from "./DirectoryTreeItem";


export function Directory(props) {
  const dispatch = useDispatch();
  const directoryState = useSelector(selectDirectory);

  useEffect(() => {
    if (directoryState.status === "idle") {
      let url = new URL(window.location.href);
      dispatch(fetchRootDirectory(Object.fromEntries(url.searchParams)))
    }
  })

  if (directoryState.status === "loading") {
    return <Loading minHeight={props.height}/>
  } else if (directoryState.status === "failed") {
    return <LoadError minHeight={props.height} message={directoryState.error}/>
  } else {
    return <div>
      <DirectoryTreeItem id={directoryState.content.id} name={directoryState.content.name} is_dir={true}
                         is_root={true}/>
      <Snackbar
        anchorOrigin={{
          vertical: 'bottom',
          horizontal: 'left',
        }}
        open={directoryState.openNonSQLWarning}
        autoHideDuration={1000}
        onClose={() => {
          dispatch(setOpenNonSQLWarning(false))
        }}
        message="Non SQL File Is Not Supported"
      />
    </div>
  }
}
