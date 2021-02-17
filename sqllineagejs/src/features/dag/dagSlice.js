import {createAsyncThunk, createSlice} from '@reduxjs/toolkit';
import {assemble_absolute_endpoint, client} from "../../api/client";

const initialState = {
  content: [],
  status: 'idle',
  error: null,
  file: ""
}

export const fetchDAG = createAsyncThunk('dag/fetchDAG', async () => {
  let url = new URL(window.location.href);
  return await client.post(
    assemble_absolute_endpoint("/lineage"),
    Object.fromEntries(url.searchParams)
  );
})

export const dagSlice = createSlice({
  name: 'dag',
  initialState,
  reducers: {
    setFile(state, action) {
      state.file = action.payload
    }
  },
  extraReducers: {
    [fetchDAG.pending]: (state) => {
      state.status = "loading"
    },
    [fetchDAG.fulfilled]: (state, action) => {
      state.status = "succeeded"
      state.content = action.payload
    },
    [fetchDAG.rejected]: (state, action) => {
      state.status = "failed"
      state.error = action.error.message
    }
  }
});

export const selectDAG = state => state.dag;
export const {setFile} = dagSlice.actions;

export default dagSlice.reducer;
