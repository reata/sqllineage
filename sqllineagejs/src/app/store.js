import {configureStore} from '@reduxjs/toolkit';
import dagReducer from '../features/dag/dagSlice';
import directoryReduce from '../features/directory/directorySlice';
import editorReducer from '../features/editor/editorSlice';

export default configureStore({
  reducer: {
    dag: dagReducer,
    directory: directoryReduce,
    editor: editorReducer,
  },
});
