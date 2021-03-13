import {createAsyncThunk, createSlice} from '@reduxjs/toolkit';
import {createSelector} from 'reselect';
import {assemble_absolute_endpoint, client} from "../../api/client";

const initialState = {
  content: {},
  status: 'idle',
  error: null
};

export const fetchDirectory = createAsyncThunk('directory/fetchDirectory', async () => {
  let url = new URL(window.location.href);
  return await client.post(
    assemble_absolute_endpoint("/directory"),
    Object.fromEntries(url.searchParams)
  );
});

export const directorySlice = createSlice({
  name: 'directory',
  initialState,
  reducers: {},
  extraReducers: {
    [fetchDirectory.pending]: (state) => {
      state.status = "loading"
    },
    [fetchDirectory.fulfilled]: (state, action) => {
      state.status = "succeeded";
      state.content = action.payload
    },
    [fetchDirectory.rejected]: (state, action) => {
      state.status = "failed"
      state.error = action.error.message
    }
  }
});

export const selectDirectory = state => state.directory;

export default directorySlice.reducer;

const directoryContentSelector = state => state.directory.content;

export const selectFileNodes = createSelector(
  directoryContentSelector,
  content => {
    let fileNodes = new Set();
    let renderResult = (nodes) => {
      Array.isArray(nodes.children) ? nodes.children.map(node => renderResult(node)) : fileNodes.add(nodes.id)
    }
    renderResult(content);
    return fileNodes;
  }
)
