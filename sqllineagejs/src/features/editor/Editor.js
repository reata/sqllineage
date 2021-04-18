import React, {useEffect} from "react";
import {useDispatch, useSelector} from "react-redux";
import {fetchContent, fetchDAG, selectEditor, setContentComposed, setEditable, setFile} from "./editorSlice";
import MonacoEditor from "react-monaco-editor";
import {Loading} from "../widget/Loading";
import {LoadError} from "../widget/LoadError";
import {useLocation} from "react-router-dom";

const useFile = () => {
  return (new URLSearchParams(useLocation().search)).get("f")
};

export function Editor(props) {
  const dispatch = useDispatch();
  const editorState = useSelector(selectEditor);
  const file = useFile();

  useEffect(() => {
    if (editorState.file !== file) {
      dispatch(setFile(file));
      if (file === null) {
        dispatch(setEditable(true));
        dispatch(fetchDAG({"e": editorState.contentComposed}))
      } else {
        dispatch(setEditable(false));
        dispatch(fetchContent({"f": file}));
        dispatch(fetchDAG({"f": file}));
      }
    }
  })

  const handleEditorDidMount = (editor, monaco) => {
    editor.onDidBlurEditorText(() => {
      const readOnly = 75;
      if (!editor.getOption(readOnly)) {
        dispatch(setContentComposed(editor.getValue()));
        dispatch(fetchDAG({"e": editor.getValue()}));
      }
    })
  }

  if (editorState.editorStatus === "loading") {
    return <Loading minHeight={props.height}/>
  } else if (editorState.editorStatus === "failed") {
    return <LoadError minHeight={props.height} message={editorState.editorError}/>
  } else {
    const options = {
      minimap: {enabled: false},
      readOnly: !editorState.editable,
      wordWrap: "on",
      automaticLayout: true
    }
    return <MonacoEditor
      width={props.width}
      height={props.height}
      language="sql"
      value={editorState.editable ? editorState.contentComposed : editorState.content}
      options={options}
      editorDidMount={handleEditorDidMount}
    />
  }
}
