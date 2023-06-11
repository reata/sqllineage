import {createAsyncThunk, createSlice} from '@reduxjs/toolkit';
import {assemble_absolute_endpoint, client} from "../../api/client";

const initialState = {
  file: "",
  dialect: "",
  content: "",
  contentComposed: "",
  editable: false,
  editorStatus: 'idle',
  editorError: null,
  dagContent: [],
  dagColumn: [],
  dagLevel: "table",
  dagVerbose: "",
  dagStatus: 'idle',
  dagError: null
}

export const fetchContent = createAsyncThunk('editor/fetchContent', async (payload) => {
  return await client.post(assemble_absolute_endpoint("/script"), payload);
})

export const fetchDAG = createAsyncThunk('dag/fetchDAG', async (payload) => {
  let dialect = localStorage.getItem("dialect");
  if (dialect !== null) {
    payload["dialect"] = dialect
  }
  return await client.post(assemble_absolute_endpoint("/lineage"), payload);
})

export const editorSlice = createSlice({
  name: 'editor',
  initialState,
  reducers: {
    setContentComposed(state, action) {
      state.contentComposed = action.payload
    },
    setEditable(state, action) {
      state.editable = action.payload
    },
    setFile(state, action) {
      state.file = action.payload
    },
    setDialect(state, action) {
      state.dialect = action.payload
    },
    setDagLevel(state, action) {
      state.dagLevel = action.payload
    }
  },
  extraReducers: {
    [fetchContent.pending]: (state) => {
      state.editorStatus = "loading"
    },
    [fetchContent.fulfilled]: (state, action) => {
      state.editorStatus = "succeeded";
      state.content = action.payload.content
    },
    [fetchContent.rejected]: (state, action) => {
      state.editorStatus = "failed"
      state.editorError = action.error.message
    },
    [fetchDAG.pending]: (state) => {
      state.dagStatus = "loading"
    },
    [fetchDAG.fulfilled]: (state, action) => {
      state.dagStatus = "succeeded";
      state.dagContent = action.payload.dag;
      state.dagVerbose = action.payload.verbose;
      state.dagColumn = action.payload.column;
    },
    [fetchDAG.rejected]: (state, action) => {
      state.dagStatus = "failed";
      state.dagError = action.error.message;
    }
  }
});

export const selectEditor = state => state.editor;
export const {setContentComposed, setDagLevel, setEditable, setFile, setDialect} = editorSlice.actions;

export default editorSlice.reducer;
