package bindings

const BlobBindingType BindingType = "blobTrigger"
// BlobBinding is the JSON representation for Blob Trigger.
type BlobBinding struct {
	Path       string `json:"path"`
	Connection string `json:"connection"`
}

// BlobTrigger is the user-facing configuration for a Blob trigger.
type BlobTrigger struct {
	Name       string
	Path       string
	Connection string
}

func (b *BlobTrigger) GetBindingType() BindingType { return BlobBindingType }

func (b *BlobTrigger) ToBinding() Binding {
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


