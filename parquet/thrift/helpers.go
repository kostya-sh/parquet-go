package thrift

func contains(a []string, b string) bool {
	for _, v := range a {
		if v == b {
			return true
		}
	}

	return false
}

func (meta *FileMetaData) GetColumnChunks(colname string) ([]*ColumnChunk, error) {
	var chunks []*ColumnChunk

	for _, rg := range meta.GetRowGroups() {
		for _, col := range rg.GetColumns() {
			if contains(col.GetMetaData().GetPathInSchema(), colname) {
				chunks = append(chunks, col)
			}
		}
	}

	return chunks, nil
}
