package message

// func TestGetMessageIDRing(t *testing.T) {
// 	ring := NewMessageIDRing()
// 	for i := minMessageID; i <= maxMessageID && i != 0; i++ {
// 		id, err := ring.GenerateID(nil)
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 		if id != i {
// 			t.Fatalf("id(%d) != i(%d), ", id, i)
// 		}
// 	}

// 	id, err := ring.GenerateID(nil)
// 	if err != merrors.ErrNoMessageIDAvailable {
// 		t.Fatalf("err expect got %v, but got %v", merrors.ErrNoMessageIDAvailable, err)
// 	}
// 	if id != 0 {
// 		t.Fatalf("id expect got 0, but got %d", id)
// 	}
// }

// func TestFreeID(t *testing.T) {
// 	ring := NewMessageIDRing()
// 	for i := minMessageID; i <= maxMessageID && i != 0; i++ {
// 		id, err := ring.GenerateID(nil)
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 		if id != i {
// 			t.Fatalf("id(%d) != i(%d), ", id, i)
// 		}
// 	}

// 	id, err := ring.GenerateID(nil)
// 	if err != merrors.ErrNoMessageIDAvailable {
// 		t.Fatalf("err expect got %v, but got %v", merrors.ErrNoMessageIDAvailable, err)
// 	}
// 	if id != 0 {
// 		t.Fatalf("id expect got 0, but got %d", id)
// 	}

// 	for i := minMessageID; i <= 100; i++ {
// 		ring.FreeFuture(i)
// 	}

// 	for i := minMessageID; i <= 100; i++ {
// 		id, err := ring.GenerateID(nil)
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 		if id != i {
// 			t.Fatalf("id(%d) != i(%d), ", id, i)
// 		}
// 	}

// 	id, err = ring.GenerateID(nil)
// 	if err != merrors.ErrNoMessageIDAvailable {
// 		t.Fatalf("err expect got %v, but got %v", merrors.ErrNoMessageIDAvailable, err)
// 	}
// 	if id != 0 {
// 		t.Fatalf("id expect got 0, but got %d", id)
// 	}
// }
