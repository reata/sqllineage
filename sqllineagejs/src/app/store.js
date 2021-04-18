import {configureStore} from '@reduxjs/toolkit';
import directoryReducer from '../features/directory/directorySlice';
import editorReducer from '../features/editor/editorSlice';

export default configureStore({
  reducer: {
    directory: directoryReducer,
    editor: editorReducer,
  },
});
