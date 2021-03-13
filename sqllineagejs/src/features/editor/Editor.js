import React, {useEffect, useRef} from "react";
import {useDispatch, useSelector} from "react-redux";
import {fetchContent, selectEditor, setFile} from "./editorSlice";
import MonacoEditor from "react-monaco-editor";
import {Loading} from "../widget/Loading";
import {LoadError} from "../widget/LoadError";
import {useLocation} from "react-router-dom";

const useFile = () => {return (new URLSearchParams(useLocation().search)).get("f")};

export function Editor(props) {
  const dispatch = useDispatch();
  const editorState = useSelector(selectEditor);
  const editorRef = useRef(null);
  const file = useFile();

  useEffect(() => {
    if (editorState.file !== file) {
      dispatch(setFile(file));
      dispatch(fetchContent());
    }
    if (editorRef.current) {
      // resize when the component is recover from hiding status
      let editor = editorRef.current.editor;
      editor.layout();
    }
  })

  if (editorState.status === "loading") {
    return <Loading minHeight={props.height}/>
  } else if (editorState.status === "failed") {
    return <LoadError minHeight={props.height} message={editorState.error}/>
  } else {
    const options = {
      minimap: {enabled: false},
      readOnly: true,
      wordWrap: "on"
    }
    return <MonacoEditor
      width={props.width}
      height={props.height}
      language="sql"
      defaultValue={editorState.content}
      options={options}
      ref={editorRef}
    />
  }
}
