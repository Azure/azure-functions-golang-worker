package bindings

const BlobBindingType BindingType = "blobTrigger"
// BlobBinding is the JSON representation for Blob Trigger.
type BlobBinding struct {
	Path       string `json:"path"`
	Connection string `json:"connection"`
}

// Blob is the user-facing configuration for a Blob trigger.
type Blob struct {
	Name       string
	Path       string
	Connection string
}

func (b *Blob) GetBindingType() BindingType { return BlobBindingType }

func (b *Blob) ToBinding() Binding {
	return Binding{
		Name:      b.Name,
		Type:      string(b.GetBindingType()),
		Direction: "in",
		BlobBinding: &BlobBinding{
			Path:       b.Path,
			Connection: b.Connection,
		},
	}
}


