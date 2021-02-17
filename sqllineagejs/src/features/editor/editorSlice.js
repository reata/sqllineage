import {createAsyncThunk, createSlice} from '@reduxjs/toolkit';
import {assemble_absolute_endpoint, client} from "../../api/client";

const initialState = {
  content: "",
  status: 'idle',
  error: null,
  file: ""
}

export const fetchContent = createAsyncThunk('editor/fetchContent', async () => {
  let url = new URL(window.location.href);
  return await client.post(
    assemble_absolute_endpoint("/script"),
    Object.fromEntries(url.searchParams)
  );
})

export const editorSlice = createSlice({
  name: 'editor',
  initialState,
  reducers: {
    setFile(state, action) {
      state.file = action.payload
    }
  },
  extraReducers: {
    [fetchContent.pending]: (state) => {
      state.status = "loading"
    },
    [fetchContent.fulfilled]: (state, action) => {
      state.status = "succeeded";
      state.content = action.payload.content
    },
    [fetchContent.rejected]: (state, action) => {
      state.status = "failed"
      state.error = action.error.message
    }
  }
});

export const selectEditor = state => state.editor;
export const {setFile} = editorSlice.actions;

export default editorSlice.reducer;
