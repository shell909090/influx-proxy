func mainx() {
	file, err := os.Open("source.dat")
	if err != nil {
		fmt.Println("open error")
		return
	}
	defer file.Close()

	var data [2048]byte
	n, err := file.Read(data[:])
	if err != nil {
		fmt.Printf("read error")
		return
	}

	buf := bytes.NewBuffer(data[:n])
	var line []byte
	var key string
	for {
		line, err = buf.ReadBytes('\n')
		switch err {
		default:
			fmt.Printf("readline error: %s\n", err)
			return
		case io.EOF, nil:
		}
		if len(line) == 0 {
			break
		}

		key, err = ScanKey(line)
		if err != nil {
			fmt.Printf("scankey error: %s\n", err)
			return
		}

		fmt.Printf("%s\n", key)
	}

}
