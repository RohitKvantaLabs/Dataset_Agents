# const datasetSchema = new mongoose.Schema({
#   title: { type: String, required: true },
#   description: { type: String, required: true },
#   source_repository: { type: String, required: true },
#   original_url: { type: String, required: true, unique: true },
#   modalities: [{ type: String }], 
#   subject_count: { type: Number },
#   species: { type: String },
#   license: { type: String },
#   is_bids_compliant: { type: Boolean, default: false },
#   quality_score: { type: Number },
#   flags: {
#     green: [{ type: String }],
#     red: [{ type: String }]
#   },
#   last_indexed: { type: Date, default: Date.now }
# });

# // Required for fast text-based searching
# datasetSchema.index({ title: 'text', description: 'text' });