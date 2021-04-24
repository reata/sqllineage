import {createAsyncThunk, createSlice} from '@reduxjs/toolkit';
import {assemble_absolute_endpoint, client} from "../../api/client";

const initialState = {
  content: {},
  status: 'idle',
  error: null,
  openNonSQLWarning: false
};

export const DirectoryAPI = async (payload) => {
  return await client.post(assemble_absolute_endpoint("/directory"), payload);
}

export const fetchRootDirectory = createAsyncThunk('directory/fetchDirectory', DirectoryAPI);

export const directorySlice = createSlice({
  name: 'directory',
  initialState,
  reducers: {
    setOpenNonSQLWarning(state, action) {
      state.openNonSQLWarning = action.payload
    }
  },
  extraReducers: {
    [fetchRootDirectory.pending]: (state) => {
      state.status = "loading"
    },
    [fetchRootDirectory.fulfilled]: (state, action) => {
      state.status = "succeeded";
      state.content = action.payload
    },
    [fetchRootDirectory.rejected]: (state, action) => {
      state.status = "failed"
      state.error = action.error.message
    }
  }
});

export const selectDirectory = state => state.directory;
export const {setOpenNonSQLWarning} = directorySlice.actions;

export default directorySlice.reducer;
