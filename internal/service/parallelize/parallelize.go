package parallelize

func Parallelize[In any](
	gorCount int, data []In, dataSize int,
	gorHandler func(
	gorIndex int,
	data []In)) {
	//wg := sync.WaitGroup{}
	//wg.Add(gorCount)

	for i := 0; i < gorCount; i++ {
		start := uint(i * dataSize / gorCount)
		end := uint((i + 1) * dataSize / gorCount)

		go func(
			gorIndex int,
			data []In) {
			//defer wg.Done()
			gorHandler(
				gorIndex,
				data)
		}(i, data[start:end])
	}

	// Waiting for all background handlers to complete.
	//wg.Wait()
}
