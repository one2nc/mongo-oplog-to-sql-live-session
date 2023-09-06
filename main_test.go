package main

import "testing"

func TestGenerateInsertSQL(t *testing.T) {
	tests := []struct {
		name    string
		oplog   string
		want    string
		wantErr bool
	}{
		{
			name:    "Empty Operation",
			oplog:   "",
			want:    "",
			wantErr: true,
		},
		{
			name: "Insert operation",
			oplog: `{
				"op": "i",
				"ns": "test.student",
				"o": {
				  "_id": "635b79e231d82a8ab1de863b",
				  "name": "Selena Miller",
				  "roll_no": 51,
				  "is_graduated": false,
				  "date_of_birth": "2000-01-30"
				}
			}`,
			want:    "INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51);",
			wantErr: false,
		},
		{
			name: "Update operation - set",
			oplog: `{
				"op": "u",
				"ns": "test.student",
				"o": {
				   "$v": 2,
				   "diff": {
					  "u": {
						 "is_graduated": true
					  }
				   }
				},
				 "o2": {
				   "_id": "635b79e231d82a8ab1de863b"
				}
			}`,
			want:    "UPDATE test.student SET is_graduated = true WHERE _id = '635b79e231d82a8ab1de863b';",
			wantErr: false,
		},
		{
			name: "Update operation - set with multiple columns",
			oplog: `{
				"op": "u",
				"ns": "test.student",
				"o": {
				   "$v": 2,
				   "diff": {
					  "u": {
						 "is_graduated": true,
						 "roll_no": 21
					  }
				   }
				},
				 "o2": {
				   "_id": "635b79e231d82a8ab1de863b"
				}
			}`,
			want:    "UPDATE test.student SET is_graduated = true, roll_no = 21 WHERE _id = '635b79e231d82a8ab1de863b';",
			wantErr: false,
		},
		{
			name: "Update operation - unset",
			oplog: `{
				"op": "u",
				"ns": "test.student",
				"o": {
				   "$v": 2,
				   "diff": {
					  "d": {
						 "roll_no": false
					  }
				   }
				},
				"o2": {
				   "_id": "635b79e231d82a8ab1de863b"
				}
			}`,
			want:    "UPDATE test.student SET roll_no = NULL WHERE _id = '635b79e231d82a8ab1de863b';",
			wantErr: false,
		},
		{
			name: "Delete operation",
			oplog: `{
				"op": "d",
				"ns": "test.student",
				"o": {
				  "_id": "635b79e231d82a8ab1de863b"
				}
			  }`,
			want:    "DELETE FROM test.student WHERE _id = '635b79e231d82a8ab1de863b';",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GenerateSQL(tt.oplog)
			if (err != nil) != tt.wantErr {
				t.Errorf("GenerateSQL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GenerateSQL() = %v, want %v", got, tt.want)
			}
		})
	}
}
