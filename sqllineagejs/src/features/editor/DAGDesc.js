import {Loading} from "../widget/Loading";
import {LoadError} from "../widget/LoadError";
import React from "react";
import {useSelector} from "react-redux";
import MonacoEditor from "react-monaco-editor";
import {selectEditor} from "./editorSlice";

export function DAGDesc(props) {
  const editorState = useSelector(selectEditor);

  if (editorState.dagStatus === "loading") {
    return <Loading minHeight={props.height}/>
  } else if (editorState.dagStatus === "failed") {
    return <LoadError minHeight={props.height} message={editorState.dagError}/>
  } else {
    const options = {
      minimap: {enabled: false},
      readOnly: true,
      wordWrap: "on",
      automaticLayout: true
    }
    return <MonacoEditor
      width={props.width}
      height={props.height}
      value={editorState.dagVerbose}
      options={options}
    />
  }
}
