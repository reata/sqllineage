import {Loading} from "../widget/Loading";
import {LoadError} from "../widget/LoadError";
import React, {useEffect, useRef} from "react";
import {useSelector} from "react-redux";
import {selectDAG} from "./dagSlice";
import MonacoEditor from "react-monaco-editor";

export function DAGDesc(props) {
  const dagState = useSelector(selectDAG);
  const editorRef = useRef(null);

    useEffect(() => {
    if (editorRef.current) {
      // resize when the component is recover from hiding status
      let editor = editorRef.current.editor;
      editor.layout();
    }
  })

  if (dagState.status === "loading") {
    return <Loading minHeight={props.height}/>
  } else if (dagState.status === "failed") {
    return <LoadError minHeight={props.height} message={dagState.error}/>
  } else {
    const options = {
      minimap: {enabled: false},
      readOnly: true,
      wordWrap: "on"
    }
    return <MonacoEditor
      width={props.width}
      height={props.height}
      defaultValue={dagState.verbose}
      options={options}
      ref={editorRef}
    />
  }
}
