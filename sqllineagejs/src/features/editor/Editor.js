import React, {useEffect} from "react";
import {useDispatch, useSelector} from "react-redux";
import {
  fetchContent,
  fetchDAG,
  selectEditor,
  setContentComposed,
  setDagLevel,
  setEditable,
  setFile,
  setDialect
} from "./editorSlice";
import MonacoEditor from "react-monaco-editor";
import {Loading} from "../widget/Loading";
import {LoadError} from "../widget/LoadError";
import {useHistory, useLocation} from "react-router-dom";

const useQueryParam = () => {
  return new URLSearchParams(useLocation().search)
};

export function Editor(props) {
  const { height, width, dialect } = props;
  const dispatch = useDispatch();
  const editorState = useSelector(selectEditor);
  const queryParam = useQueryParam();
  const history = useHistory();

  useEffect(() => {
    let query = queryParam.get("e");
    if (query !== null) {
      dispatch(setContentComposed(query));
      history.push("/");
    } else {
      let file = queryParam.get("f");
      if (editorState.file !== file || editorState.dialect !== dialect) {
        dispatch(setFile(file));
        dispatch(setDialect(dialect));
        dispatch(setDagLevel("table"));
        if (file === null) {
          dispatch(setEditable(true));
          dispatch(fetchDAG({"e": editorState.contentComposed}))
        } else {
          dispatch(setEditable(false));
          dispatch(fetchContent({"f": file}));
          dispatch(fetchDAG({"f": file}));
        }
      }
    }
  })

  const handleEditorDidMount = (editor, monaco) => {
    const readOnly = monaco.editor.EditorOption.readOnly;
    editor.onDidBlurEditorText(() => {
      if (!editor.getOption(readOnly)) {
        dispatch(setContentComposed(editor.getValue()));
        dispatch(fetchDAG({"e": editor.getValue()}));
      }
    })
    editor.onKeyDown(() => {
      // This is a walk-around to trigger "Cannot editor in readonly editor". Be default this tooltip is only shown
      // when user press backspace key on readonly editor, we want it with any key
      if (editor.getOption(readOnly)) {
        editor.trigger(monaco.KeyCode.Backspace, 'deleteLeft')
      }
    })
  }

  if (editorState.editorStatus === "loading") {
    return <Loading minHeight={height}/>
  } else if (editorState.editorStatus === "failed") {
    return <LoadError minHeight={height} message={editorState.editorError}/>
  } else {
    const options = {
      minimap: {enabled: false},
      readOnly: !editorState.editable,
      wordWrap: "on",
      automaticLayout: true
    }
    return <MonacoEditor
      width={width}
      height={height}
      language="sql"
      value={editorState.editable ? editorState.contentComposed : editorState.content}
      options={options}
      editorDidMount={handleEditorDidMount}
    />
  }
}
